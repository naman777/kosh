# Kosh — Implementation Plan

This turns `Payments Core — Build Plan.md` into build steps. The build plan says *what* and *why*; this file says *how*, in the order it will be built. Progress is tracked in [progress.md](progress.md).

Module: `github.com/naman777/kosh` · Go 1.25 · PostgreSQL 16 · Redpanda (Kafka API)

## Ground rules

- **Money is `money.Paise` (int64) everywhere.** No floats in any code path that touches an amount. Arithmetic that can overflow returns an error.
- **Postgres is the source of truth.** Every money movement and the outbox event that announces it commit in one transaction. Nothing leaves the process before that commit.
- **Ledger rows are append-only.** Corrections are new entries. Triggers enforce invariant 11, so a bug cannot quietly edit history.
- **Every invariant is one SQL file** in `invariants/`, loaded by a Go checker that tests, the simulation harness and CI all share.
- **Nondeterminism goes behind an interface from day one** (`clock.Clock`, `rng.RNG`, `bank.Client`, the webhook HTTP sender), even before the simulation harness exists, so week 5 doesn't need a refactor.
- **Minimal dependencies:** `pgx/v5` for Postgres, the standard library `net/http` (Go 1.22 routing) for the API, `franz-go` for Kafka. No ORM and no web framework.
- **Tests run against real Postgres.** Each test gets a fresh database cloned from a migrated template, so tests are isolated and fast. CI runs everything with `-race`.

## Phase 0 · Scaffold

| Task | Detail |
| --- | --- |
| Repo | `git init`, `go.mod`, `.gitignore`, `.editorconfig` |
| Compose | `docker-compose.yml` with Postgres 16 (host port 5442, so it can't clash with other local stacks). Redpanda is added in phase 3. |
| Migrations | Plain numbered SQL files in `migrations/`, embedded with `go:embed`, applied by a small runner (`internal/db/migrate.go`) that keeps a `schema_migrations` table. No migration tool to install. |
| Test DB helper | `internal/testdb`: creates `kosh_template` once per run (migrated), then `CREATE DATABASE … TEMPLATE kosh_template` per test and drops it afterwards. Reads `KOSH_TEST_DATABASE_URL`. |
| CI | GitHub Actions: Postgres service container, `go vet`, `go test -race ./...` |
| Makefile | `make up`, `make test`, `make sim SEED=…` for Linux/CI. On Windows the README lists the equivalent `go` commands. |

## Phase 1 · Ledger core (build plan week 1)

**`internal/money`**

- `type Paise int64`, with `Add`, `Sub` and `MulBps` returning errors on overflow, and string formatting as `₹1,000.00`.
- `Fees(amount, feeBps) → {Fee, GST, Net}`. The rounding rules are written in the package doc and tested:
  - fee = amount × bps / 10,000, rounded half-up to the paisa
  - GST = 18% of the fee, rounded half-up to the paisa
  - net = amount − fee − GST. The merchant line absorbs rounding, so the lines always sum exactly to the amount.
- Table tests for the README example (₹1,000 at 2% → 97,640 / 2,000 / 360) and edge cases (1 paisa, odd amounts, max int64).

**Schema (`migrations/0001_ledger.sql`)**: `accounts`, `journal_entries` and `postings` as in the build plan, plus:

- `accounts`: `UNIQUE (code, owner_id)` and `CHECK` constraints that every balance column is ≥ 0.
- `postings`: an index on `account_id` and on `entry_id`.
- Triggers: `postings` reject every `UPDATE` and `DELETE`. `journal_entries` allows only `pending → posted` and `pending → voided`, and rejects `DELETE`.

**`internal/ledger`**

- Chart of accounts: system accounts (`bank_receivable`, `fee_revenue`, `gst_payable`, `bank_settlement`, `payout_clearing`) plus a `merchant_payable` per merchant, created with the merchant.
- `Flags`: `DebitsMustNotExceedCredits` and `CreditsMustNotExceedDebits`, checked against *posted + pending* balances, so a hold counts.
- `Ledger.Post(ctx, tx, Entry)` validates (at least two postings, each amount > 0, debits = credits), locks the touched accounts with `SELECT … FOR UPDATE ORDER BY id`, checks flags, then inserts the entry and postings and updates balances, all inside the caller's transaction. Callers pass a `pgx.Tx`, so payments can commit ledger, payment state and outbox together.
- Holds: `Ledger.Hold` writes a `pending` entry and moves only `pending_dr`/`pending_cr`. `Commit(entryID)` and `Void(entryID)` use `UPDATE journal_entries SET state = … WHERE id = $1 AND state = 'pending'`. Zero rows updated means the hold was already resolved, so it returns `ErrHoldResolved` without moving any balance twice.
- `ExpireHolds(now)` voids pending entries past `expires_at`.
- Errors are typed sentinel values (`ErrUnbalanced`, `ErrInsufficientFunds`, `ErrDuplicateEntry`, …) so the API can map them to HTTP codes.

**`invariants/`** — SQL files for 1, 2 and 11. Each one returns violating rows, and an empty result means it passes. `invariants.Check(ctx, db)` runs them all and returns a report.

- 1: entries whose debits ≠ credits, plus a whole-ledger total.
- 2: accounts whose stored `posted_*`/`pending_*` differ from the sum of their postings, grouped by entry state.
- 11: the protective triggers exist and are enabled. A test also tries the forbidden `UPDATE`/`DELETE` and expects an error.

**Tests**

- Unit tests for validation and the money math.
- Property test, pure: 1,000,000 random transfers through the validation and balance model, checking that debits equal credits after each one. This runs in seconds.
- Property test, DB-backed: N random transfers (default 20,000; `-transfers=1000000` for the full run) across random accounts, then the invariant checker.
- Concurrency test: 50 goroutines moving money among 5 accounts, crossing in both directions to provoke deadlocks, under `-race`. Expected: no deadlock errors, invariants pass, and no flagged account goes negative.
- Hold test: commit and void race on the same hold. Exactly one wins.

**Done when** the property and concurrent-transfer tests pass with `-race`.

## Phase 2 · Payments and idempotency (build plan week 2)

**Schema (`0002_payments.sql`)**: `merchants`, `api_keys` (SHA-256 hash of the key, never the key itself), `orders`, `payments`, `refunds`, `idempotency_keys`.

**Additions to the build-plan schema:**

- `orders` and `refunds` tables. The plan references both but doesn't define them.
- `payments.version INT`, which increments on every state change. Phase 3 uses it to prove invariant 7.
- `idempotency_keys` gets `recovery_point`, `locked_at` and `resource_id`, so a request that crashed halfway can resume (see below).

**Payment state machine (`internal/payments`)**

```
created → pending → authorized → captured → (partially_)refunded → refunded
   │         │          │
   └────→ failed ←──────┘   (authorization expiry)
```

- Transitions live in one table in code, and every change goes through `transition(tx, id, from, to)`, which runs `UPDATE payments SET state = $to, version = version + 1 WHERE id = $id AND state = $from`. Zero rows means a lost race or an illegal transition, and the function returns a typed error.
- Capture, in one transaction: transition `authorized → captured`, then post the capture entry (Dr `bank_receivable`; Cr `merchant_payable`, `fee_revenue`, `gst_payable`). `UNIQUE (kind, ref_id)` makes a second capture impossible even if the guard had a bug (invariant 3).
- Refund, partial or full:
  1. In a transaction: `UPDATE payments SET refunded = refunded + $x WHERE id = $id AND refunded + $x <= amount AND state IN (…)`, insert the refund row, and place a hold (Dr `merchant_payable`, Cr `bank_receivable`, pending). The merchant-payable flag stops a refund the merchant can't cover (invariant 5). The guarded update makes concurrent refunds safe (invariant 4).
  2. Call the bank refund outside the transaction.
  3. Commit the hold on success, or void it and give back `refunded` on failure, in one transaction.

**Idempotency (`internal/idempotency`)** — middleware for every `POST`, following Stripe's "atomic phases" pattern:

- The key is `(merchant_id, Idempotency-Key header)` and the request hash is SHA-256 of method + path + body.
- Phase A, one transaction: insert the key as `in_progress` and run the local work (for example, create the payment as `created`). If the key already exists, compare hashes: a mismatch returns 422 and writes nothing; `done` replays the stored response; `in_progress` with a fresh `locked_at` returns 409 (a concurrent duplicate).
- Phase B: the external call (the bank), which is idempotent on our payment id.
- Phase C, one transaction: apply the result, store the response, and mark the key `done`.
- If the process crashes between phases, the key stays `in_progress` with a `recovery_point`. Once `locked_at` is stale, a retry resumes from that point instead of starting over. This is how "retry any call 5 times → one payment, one capture" holds even across a crash.

**Bank (`internal/bank` + `cmd/banksim`)**

- `bank.Client` interface: `Authorize`, `Capture`, `Refund`, `Status`, each keyed by our id so retries are safe.
- `cmd/banksim`: an HTTP server whose behaviour is set per request or globally through an admin endpoint: success rate, failure rate, timeout rate, and latency distribution. It returns a `bank_ref` (UTR/RRN) and dedupes by our id.
- `bank.Fake`: an in-process implementation for unit tests with a scripted outcome per call.

**API (`cmd/api`)**: `POST /v1/orders`, `POST /v1/payments`, `GET /v1/payments/{id}`, `POST /v1/payments/{id}/capture`, `POST /v1/payments/{id}/refunds`, plus `POST /admin/merchants` (returns the API key once). Auth is `Authorization: Bearer <key>`. Errors are JSON with a stable `code`.

**Invariants:** add 3, 4, 5 and 6 as SQL. Invariant 6 is also checked by tests that replay requests.

**Tests**

- The state machine table: every illegal transition is rejected.
- A retry storm: the same request 5× sequentially and 5× concurrently produces one payment, one capture and identical response bodies.
- The same key with a different body returns 422 and leaves no new rows.
- 10 concurrent refunds on one payment: `refunded ≤ amount`, the extras are rejected, and invariants hold.
- Crash between phases A and C (using a crashpoint), then a retry, produces exactly one payment.

**Done when** a client retrying any call 5 times never creates a second payment or capture.

## Phase 3 · Events and webhooks (build plan week 3) → Gate 1

**Outbox (`internal/outbox`)**

- `outbox.Write(tx, aggregateID, version, eventType, payload)` is called from `transition()` and from refund state changes, so it is impossible to change state without writing an event (invariant 7).
- `UNIQUE (aggregate_id, aggregate_version)`. The invariant 7 query checks that each payment has exactly `version` events, numbered 1…version.
- Relay (`cmd/relay`): a loop running `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`, produce to Redpanda topic `kosh.events` (keyed by aggregate id, so a payment's events stay ordered), wait for the acks, then set `published_at`. A crash after producing and before marking means a duplicate publish, which at-least-once allows; consumers dedupe by event id.

**Webhooks (`internal/webhooks`, `cmd/dispatcher`)**

- The Kafka consumer materialises each event into `webhook_deliveries` (one row per enabled merchant, `ON CONFLICT DO NOTHING`) and only then commits the offset.
- The sender loop claims due rows (`next_attempt_at <= now() … FOR UPDATE SKIP LOCKED`), POSTs them and records the result.
- Signature header: `Kosh-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, t + "." + body)>`. The event id goes in a `Kosh-Event-Id` header and in the body.
- Retries use exponential backoff with full jitter (base 10 s, cap 1 h, from the injectable RNG and clock). When a merchant has had no successful delivery for 24 h, set `webhook_disabled_at`.
- `POST /v1/webhooks/replay` (by event-id range or time range) re-enables the merchant and resets those deliveries.
- `cmd/samplemerchant`: verifies the signature with a 5-minute tolerance, dedupes by event id, and can be told to return 500 or hang (for the failure lab).

**Crashpoints (`internal/crash`)**: `crash.Point("relay.after_publish_before_mark")` exits the process with code 137 when `KOSH_CRASHPOINT` names that point. Tests build the binaries, run them as subprocesses with a crashpoint set, restart them, and assert that nothing was lost. Phase 5's harness reuses the same package.

**Invariants:** add 7 and 8. Invariant 8 is "eventually": the SQL checks published events older than a grace window, and tests run to quiescence and then check strictly.

**Gate 1 deliverables**

- `README.md`: pitch, architecture diagram (Mermaid), the ₹1,000 worked example, invariants list, how to run it, what's out of scope, and the first numbers (a Go benchmark of ledger transfers/s on this laptop, with the hardware stated).
- `docs/DESIGN.md` started, with the first ADRs: Postgres vs TigerBeetle, outbox vs CDC, pessimistic locking.
- Invariants 1–8 pass in CI.

## Phase 4 · Bank chaos and settlement (build plan week 4)

Lessons from phases 1–3 that shape this one: every bank answer has to go through *one* function, because the same answer can now arrive three ways (the API's own call, a callback, the poller). And invariants that talk about time need to use the test's clock, not the database's `now()`.

**Schema (`0004_settlement.sql`)**

- `payments`, `refunds`: `next_poll_at` and `poll_attempts` for the pollers. `refunds.kind` is `merchant` or `auto`.
- `payouts`: one row per merchant per settlement date (`UNIQUE (merchant_id, settlement_date)`), with a hold, a state (`pending → processed | failed`), a version and outbox events.
- New system account `customer_refund_payable` (liability): money a late success owes back to a customer.
- `outbox.aggregate_type` allows `payout`.

**One resolver for bank answers (`applyBankResult`)**

| Payment is | Bank says success | Bank says failed | Bank says pending / unknown |
| --- | --- | --- | --- |
| `created` / `pending` | → `authorized` | → `failed` | → `pending`, poll again |
| `failed` | **late success**: record it, auto-refund | nothing | nothing |
| `authorized` / `captured` / `refunded` | nothing (duplicate) | logged, nothing | nothing |

A late success posts `late_success` (Dr `bank_receivable`, Cr `customer_refund_payable`), then an `auto` refund holds and posts the reverse once the bank confirms. The merchant gets `refund.created` / `refund.processed` events, and the payment stays `failed` but gains its `bank_ref`, so phase 5 recon can match the settlement line.

The create-payment phase C, the bank callback endpoint, and the poller all call this resolver. Refunds and payouts get the same treatment.

**Bank callbacks.** `POST /bank/callbacks` is signed with HMAC (`Bank-Signature`) and carries `{kind, id, status, bank_ref, reason}`. It is idempotent, so the bank can send it five times.

**Pollers (`cmd/worker`)**

- Payments in `created`/`pending` past `next_poll_at`: claim with a lease (`SKIP LOCKED`), ask `bank.Status`, resolve. Backoff from 5 s to 5 min. A payment still pending after 30 min is marked `failed` (`bank_timeout`); if the bank succeeds later, that is the late-success path.
- `created` payments are first polled 2 min after creation, past the idempotency lease, so the poller only acts on requests that really died.
- Refunds and payouts: the same loop. Holds are voided only by the expiry job, after the poller has had its chance.

**T+1 settlement and payouts**

- For settlement date D, each merchant's eligible amount is available balance (posted credits − posted debits − pending debits) minus credits posted on or after the start of D+1 IST. That nets out refunds and open holds, and leaves today's captures for tomorrow. A run that was skipped is covered by the next one.
- The batch is idempotent through `UNIQUE (merchant_id, settlement_date)`. Each payout runs in phases like a payment: (A) hold Dr `merchant_payable` / Cr `payout_clearing` plus a payout row, (B) `bank.Payout`, (C) on success commit the hold and post Dr `payout_clearing` / Cr `bank_settlement`; on failure void the hold. A failed payout's money is picked up by the next day's batch.

**Bank simulator v2**

- Per-request answers with a `final_at` time: `pending` until then, final after. Callbacks are sent to Kosh after `callback_delay_ms`, `1..max_duplicate_callbacks` times.
- `outage: true` returns 503 and records nothing.
- `POST /v1/payouts`.
- `GET /v1/settlements/{date}`: a CSV of the day's successful authorizations and refunds (`line_no, bank_ref, reference, gross, fee, tax, net, value_date`), with seeded fault injection (fee variance, missing, duplicate and amount-mismatch rows). `GET /admin/settlements/{date}/faults` returns the injected truth, so phase 5 can score recon accuracy.

**Invariants**

- 9 as SQL: no pending hold past its deadline (plus 10 min for the expiry job), no hold posted after its deadline, and every refund and payout hold matches its row's state.
- `invariants.CheckAt(now)` passes the test's clock to SQL through `set_config('kosh.now', …)`. Tests on a fake clock stop depending on the real date.
- 4, 6 and 7 extended for auto refunds and payouts.

**Tests**

- Banksim: dedupe, outage, pending → final, duplicate callbacks, deterministic settlement files.
- Resolver table: every (state, answer) pair.
- Late success: timeout → pending → marked failed at 30 min → success callback → auto refund processed. `customer_refund_payable` ends at zero.
- A payment request crashes after insert, the poller fails it, and the client's retry gets a bank success: auto refund, no money lost.
- 5 duplicate callbacks give one transition and one event.
- Settlement: the eligible-amount math, a batch run twice gives one payout, payout failure restores the balance, and the next day's batch pays.
- **Done-when test (`TestBankChaos`):** API + banksim (all faults on, callbacks to the API) + worker loops over HTTP, with many payments, refunds and settlements. Afterwards, invariants 1–9 hold.

## Phase 5 · Reconciliation and simulation (build plan week 5)

Lessons from phase 4: the bugs worth finding show up when real faults overlap (a late callback crossing a hold deadline, a capture racing a settlement). A deterministic harness that throws thousands of seeded combinations at the system, and can replay any failure, is how to find the next ones on purpose instead of by luck.

### Reconciliation (`internal/recon`)

**Schema (`0005_recon.sql`)**

- `settlement_files` (one per business date, with a checksum so re-ingesting the same file is a no-op and a different file for the same date is refused) and `settlement_lines`.
- `recon_results`: one row per settlement line, plus one per ledger item the bank never settled. Each has a status, the expected and actual amounts, its effect on the suspense account, and `resolved_by_entry` / `resolved_at`. The **exception queue** is every non-`matched` row that is not resolved.
- `payments.bank_confirmed_at` and `refunds.bank_confirmed_at`: when Kosh learned the bank succeeded. This is the "T" in T+2.
- New accounts: `bank_charges` (expense), `recon_suspense` (variances waiting for a human), `recon_writeoff` (expense).

**Matching and the six statuses.** For a file, each line is matched against the pool of bank-confirmed payments and refunds not yet matched: first by `bank_ref`, then by Kosh's reference id (the bank echoes it).

| Status | When | What gets posted |
| --- | --- | --- |
| `matched` | found; gross and fee agree | Cr `bank_receivable` gross, Dr `bank_charges` expected fee, Dr `bank_settlement` net |
| `fee_variance` | found; gross agrees, fee differs from `bank.Fee` | as matched, and the difference goes to `recon_suspense` |
| `amount_mismatch` | found; gross differs | receivable cleared at the expected gross, cash at actual, difference to suspense |
| `duplicate` | bank_ref already settled (in this file or an earlier one) | Dr `bank_settlement` / Cr `recon_suspense` (extra cash) |
| `missing_in_ledger` | no ledger item | Dr `bank_settlement` / Cr `recon_suspense` (unexplained cash) |
| `missing_in_bank` | a ledger item confirmed before the file's date still unmatched | nothing yet; the receivable stays open |

Each file is reconciled once, in one transaction that writes all results and **one** `bank_settlement` entry for the file. This is where cash finally arrives in `bank_settlement`. A missing item that turns up in a later file is matched then, and its `missing_in_bank` exception auto-closes.

**Resolving exceptions** posts an adjusting entry and never edits anything: `write_off` (suspense or receivable to `recon_writeoff`) or `bank_corrected` (the bank returned or paid the difference: suspense against `bank_settlement`). Each exception resolves exactly once.

**Invariant 10 (SQL):** for the latest reconciled business date D, every bank-confirmed item from before D has a result (matched or `missing_in_bank`). Every line of a reconciled file has exactly one result. `recon_suspense`'s balance equals the sum of open exceptions' suspense effects.

**Accuracy metric.** Banksim's truth endpoint lists the faults it injected. A test reconciles seeded files and scores injected faults classified correctly, plus clean lines matched. The target is 98%.

### Settlement file format (shared)

The file builder moves out of the banksim server into `banksim.BuildSettlement(records, date, …)` with `WriteCSV` / `ParseCSV`. The HTTP simulator and the simulation harness's in-process bank produce byte-identical files, and recon reads them through the same parser.

### Simulation harness (`sim/`, `cmd/sim`)

- **Determinism.** One seed drives the workload RNG, the id generator, the dispatcher's jitter, a fake clock and an in-process **sim bank**. The sim bank decides success, failure, timeout, pending-then-final and outage per request, and queues callbacks (late, 1–4 copies) to deliver at a simulated time. One goroutine runs everything in order, so the same seed gives the same run. Postgres is real (a fresh clone per seed); its own scheduling isn't replayed, but with one goroutine nothing races.
- **Each step is one action picked by the RNG:** new order and payment, retry the same request (same key), capture, partial refund, two refunds on one payment back to back, deliver due callbacks, advance the clock (seconds to hours), worker round, relay and dispatcher round, flip the merchant endpoint between up and down, toggle a bank outage, ingest and reconcile a settlement file.
- **Crashes.** Some steps arm a random named crashpoint first. The action panics there, the harness "restarts" (recreates the bus consumer from its committed offset and forgets in-flight state), and the client retries later with the same key.
- **Checks.** After every step, invariants 1–7, 9, 10 and 11 run. At the end the harness turns faults off, drains (advancing the clock through settlement and reconciliation), and checks all 11, including 8.
- **Replay.** Every step appends a line to a trace whose hash is reported. A failing seed prints `go run ./cmd/sim -seed N` (or `make sim SEED=N`), and `-v` prints the trace. A test checks that one seed run twice gives the same trace hash.
- **Scale.** `cmd/sim -seeds 10000 -parallel 8` runs seeds concurrently, each on its own database. CI runs 200 (`KOSH_SIM_SEEDS=200 go test ./sim`), and a plain `go test ./...` runs 20.
- `testdb` gains a non-test API (`Clone`) so `cmd/sim` and `ledgerbench` don't fake a `testing.TB`.

**Done when** 10,000 seeds pass all 11 invariants and recon classifies the injected faults (≥ 98%).

## Phase 6 (planned in detail later)

- **Phase 6 · Load, observability, launch:** k6 scenarios, Prometheus/Grafana, the Next.js failure lab, GCP deploy, README numbers, demo video.

Each gets its own detailed section here when it starts.

## Working agreement

- One small commit per finished checklist item, authored by Naman only.
- `progress.md` is updated as each item finishes: status, what was built, decisions, and any bug a test caught (for the interview "bugs the harness caught" story).
- Anything that changes the build plan's design gets a line under "Decisions" in `progress.md`.
