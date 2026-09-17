# Kosh — Progress

Plan: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) · Spec: [Payments Core — Build Plan.md](Payments%20Core%20—%20Build%20Plan.md)

**Current phase:** Phase 4 done. Next: Phase 5 · Reconciliation and simulation
**Last updated:** 2026-10-17

## Status

| Phase | Status |
| --- | --- |
| 0 · Scaffold | done |
| 1 · Ledger core | done (2026-10-17) |
| 2 · Payments and idempotency | done (2026-10-17) |
| 3 · Events and webhooks → Gate 1 | done (2026-10-17) |
| 4 · Bank chaos and settlement | done (2026-10-17) |
| 5 · Reconciliation and simulation | not started |
| 6 · Load, observability, launch | not started |

## Phase 0 · Scaffold

- [x] git repo, `go.mod`, `.gitignore`
- [x] Docker Compose with Postgres 16 (host port 5442)
- [x] Embedded migration runner (`internal/db`)
- [x] Test database helper (`internal/testdb`): template database keyed by a hash of the migrations, cloned per test, dropped afterwards
- [x] CI workflow (`go vet`, `go test -race`). Not yet run on GitHub, since the repo isn't pushed.

## Phase 1 · Ledger core

- [x] `money` package with written rounding rules: half-up for fee and GST, and the merchant's net line absorbs rounding so lines always sum
- [x] Ledger schema and append-only triggers (`migrations/0001_ledger.sql`)
- [x] Ledger service: post, holds (commit/void/expire), account flags (`internal/ledger`)
- [x] Invariant checker v0 (1, 2, 11) in `invariants/`
- [x] Property test: 1M pure steps (validation + flags against an in-memory model) and a DB-backed run (5,000 ops by default, `-transfers=1000000` for the full run)
- [x] Concurrent transfer and hold-race tests under `-race`

**Done-when check:** `go test -race ./internal/ledger` passes, 14 tests in ~32 s on the laptop.

Notes:

- The pure property run: 1,000,000 steps → 732,705 accepted, 2,187 refused by flags, 81,603 malformed entries rejected.
- `TestConcurrentTransfers` (50 goroutines, crossing transfers among 5 accounts): 1,798 posted, 202 refused for insufficient funds, no deadlocks, money conserved.
- `TestInvariantsCatchDrift` corrupts a balance on purpose to prove the checker can fail.
- The full 1M DB-backed run (`make test-full`) has not been run yet.

## Phase 2 · Payments and idempotency

- [x] Merchants, API keys (SHA-256 stored), orders, payments, refunds schema (`migrations/0002_payments.sql`)
- [x] Payment state machine with guarded transitions (`internal/payments/state.go`): one `transition()` for every state change
- [x] Idempotency store (`internal/idempotency`): atomic phases, recovery points, lease ids as fencing tokens, byte-identical replays
- [x] Bank simulator v1 (`cmd/banksim`) with fail/timeout rates and latency, plus `bank.Fake` for tests
- [x] Capture posts merchant net, fee and GST lines; zero lines dropped for 0% merchants
- [x] Partial and full refunds with ledger holds; holds expire after 72 h and fail the refund
- [x] Invariants 3–6, retry-storm, concurrent-refund, crash-between-phases and end-to-end HTTP tests
- [x] HTTP API (`cmd/api`): orders, payments, capture, refunds, admin merchant onboarding

**Done-when check:** `TestRetryStorm` retries create-payment 5× sequentially and capture 5× concurrently: one payment, one capture entry, one bank call, identical bytes on every replay. `TestEndToEnd` repeats it over real HTTP.

Notes:

- Fees are not returned on refunds (Razorpay behaviour), so a merchant whose only sale was ₹1,000 cannot refund all of it: the payable holds ₹976.40. The refund is refused with 422 `insufficient_balance`. `TestHappyPath` covers this.
- A bank timeout leaves the payment `pending`, never `failed`. The poller that resolves pending payments is phase 4.
- Only one live (non-failed) payment per order, enforced by a partial unique index as well as the order row lock.
- 4xx errors raised after a key is acquired are stored and replayed like successes (Stripe's semantics). 5xx errors release the key so a retry resumes immediately.

## Phase 3 · Events and webhooks → Gate 1

- [x] Transactional outbox (`migrations/0003_events.sql`, `internal/outbox`): written by every payment and refund state change, `UNIQUE (aggregate_id, aggregate_version)`
- [x] Relay with `FOR UPDATE SKIP LOCKED` (`cmd/relay`), Redpanda in compose, franz-go client with acks=all
- [x] Webhook dispatcher (`cmd/dispatcher`): HMAC-SHA256 over `t.body`, backoff from 10 s to a 1 h cap with equal jitter, claim leases, 24 h disable, `POST /v1/webhooks/replay`
- [x] Sample merchant receiver (`cmd/samplemerchant`): verifies signatures, dedupes by event id, ok/fail/hang modes
- [x] Crashpoints (`internal/crash`) with in-process crash tests, plus `e2e/kill_test.go`, which kills the real binaries against Redpanda
- [x] Invariants 7–8
- [x] README with numbers and hardware, `docs/DESIGN.md`, ADRs 0001–0004
- [x] Live smoke run of all five binaries with the README's curl walkthrough (5 webhooks received and verified, retry byte-identical, key reuse → 422)

**Gate 1 check:** invariants 1–8 pass in every test; `go test -race ./...` passes with `KOSH_KAFKA_BROKERS` set (e2e included). CI is written but has not run on GitHub yet, since the repo isn't pushed.

## Phase 4 · Bank chaos and settlement

Detailed plan: [IMPLEMENTATION_PLAN.md § Phase 4](IMPLEMENTATION_PLAN.md#phase-4--bank-chaos-and-settlement-build-plan-week-4).

- [x] Schema (`migrations/0004_settlement.sql`): poll scheduling columns, `refunds.kind` (merchant | auto), `payouts`, `customer_refund_payable` account
- [x] Invariant 9 as SQL, and `invariants.CheckAt` so fake-clock tests pass their "now" to SQL; invariants 4, 6, 7 extended for auto refunds and payouts
- [x] Bank simulator v2: `pending` answers decided later, outages (503, nothing recorded), signed callbacks sent late and 1–N times, payouts, references unique across restarts
- [x] One resolver per aggregate for bank answers (API phase C, `POST /bank/callbacks`, pollers all use it)
- [x] Pollers for payments, refunds and payouts with lease claims and backoff; 30 min pending timeout for payments; late success → auto refund
- [x] T+1 settlement batch (`RunSettlement`), two-phase payouts, payout poller, bank-checked hold expiry
- [x] Settlement files with injected fee-variance, missing, duplicate and amount-mismatch rows, plus a truth endpoint for scoring recon
- [x] `cmd/worker`, `POST /admin/settlements/run`, `GET /v1/payouts/{id}`
- [x] `TestBankChaos`: API + worker + faulty bank over HTTP, then a day jump for settlement; all invariants hold. It has passed 10 consecutive runs since the last fix.
- [x] Live smoke run with real binaries: pending → duplicate signed callbacks → authorized; forged callback → 401

**Done-when check:** invariants 5 and 9 (and all the others) hold after `TestBankChaos`, whose bank declines, times out, answers pending, goes down and duplicates callbacks. A typical run: 99 payments, ~40 captured, ~58 failed, ~15 late successes auto-refunded, ~250 callbacks applied, 3 payouts.

Notes:

- Payouts never fail just for staying pending; only their 7-day hold expiry ends them, after asking the bank. Failing a payout the bank later completes would pay twice.
- Auto refunds the bank declines stay as a balance in `customer_refund_payable`. Phase 5's exception queue will surface them.

## Decisions

Changes to, or details missing from, the build plan.

- 2026-10-17 · Module path `github.com/naman777/kosh`.
- 2026-10-17 · Postgres runs on host port 5442 (5433 is taken by the convo-ai stack).
- 2026-10-17 · Go 1.25 (pgx v5.9+ requires it). The local Go 1.23 install fetches the 1.25 toolchain automatically via `GOTOOLCHAIN=auto`.
- 2026-10-17 · Migrations are embedded SQL with a tiny runner, not a migration tool.
- 2026-10-17 · Added `orders`, `refunds`, `api_keys`, `payments.version` and idempotency recovery columns (see the plan, phase 2).
- 2026-10-17 · `idempotency_keys.response` is `JSON`, not `JSONB`. JSONB re-orders keys, which would break byte-identical replays.
- 2026-10-17 · Idempotency leases are fenced by a random `lease_id`, not `locked_at`. Two takeovers in the same clock tick (common with the fake clock) would otherwise share a token.
- 2026-10-17 · Capture is ledger-only (no bank call), as with Razorpay: the bank already collected the money at authorization.
- 2026-10-17 · Refunds credit `bank_receivable`, since the bank nets refunds out of settlement. This will be revisited when settlement lands in phase 4.
- 2026-10-17 · franz-go pinned to v1.18.1. Newer releases require Go 1.26.
- 2026-10-17 · The API's default port is 8090 (8080 is taken on the dev laptop); banksim 8091, sample merchant 8092, Redpanda 19092.
- 2026-10-17 · Webhook retry uses "equal jitter" (half fixed, half random) instead of full jitter, so a retry never fires almost instantly.
- 2026-10-17 · The Kafka consumer session timeout is 6 s (Redpanda's minimum) so a crashed dispatcher's partitions move quickly.
- 2026-10-17 · Invariant 8 is strict and meant to be checked at quiescence; the phase 5 harness will need a "settled" notion before checking it mid-run.
- 2026-10-17 · A late success after `failed` keeps the payment `failed` (so invariant 3 stays simple) but records its `bank_ref`, posts `late_success`, and creates an `auto` refund. The merchant sees `refund.created` / `refund.processed` events.
- 2026-10-17 · Refunds and payouts the bank never received are *submitted* by the poller (Kosh owes them). Payments the bank never received are *failed* (Kosh can't complete them without the customer).
- 2026-10-17 · A hold's deadline means "stop waiting and decide". Bank-confirmed commits land even after it (ADR 0005).
- 2026-10-17 · The payout entry credits `bank_settlement` (cash out). It runs negative until phase 5 posts the bank's incoming settlements.
- 2026-10-17 · The bank charges Kosh 0% on UPI and 1% + 18% GST on cards (`bank.Fee`). Settlement files deduct it; recon will check it.
- 2026-10-17 · `api.New` takes a `Config` (admin token, bank callback secret).

## Bugs caught by tests

The interview story. Every bug a test, invariant or the harness caught, with the date and the fix.

1. **2026-10-17 · Relay could not restart.** Caught by `e2e/kill_test.go`: after the first crash, the restarted relay died with `TOPIC_ALREADY_EXISTS`. kadm reports a per-topic error both in `resp.Err` and as the call's own error, and `EnsureTopic` only tolerated the first. Every restart of the relay or dispatcher would have failed in production. Fix: tolerate it in both places (`internal/bus/kafka.go`).
2. **2026-10-17 · Dispatcher recovery took ~45 s.** Caught by the same test's runtime (63 s): a killed consumer keeps its partitions until Kafka's default 45 s session timeout expires, which breaks the 10 s recovery target. Fix: 6 s session timeout with a 1 s heartbeat. The test dropped to 29 s.
3. **2026-10-17 · Every capture is single-file.** Caught by `ledgerbench`: "spread" and "hot" modes gave identical throughput because both debited `bank_receivable`. Real captures touch three shared system accounts, so they serialize: 278/s with p99 576 ms, against 1,928/s for unrelated transfers. This isn't a correctness bug; it is the hot-account problem, now measured (ADR 0003). The fix is planned for phase 6.
4. **2026-10-17 · Late bank confirmations were rejected.** Caught by `TestBankChaos` on its first run: a refund's bank success arrived after its hold's deadline, `CommitHold` refused it (`hold expired`), and the callback and worker failed on it forever. The expiry job would then have voided the hold, recording "refund failed" while the customer had the money. Fix: `CommitConfirmed` for bank-confirmed commits, expiry asks the bank before voiding, and invariant 9 lost a check that encoded the same mistake (ADR 0005).
5. **2026-10-17 · Yesterday's payout included today's sales.** Caught by `TestBankChaos` (a payout for a day with no sales). Under READ COMMITTED, `SELECT … FOR UPDATE` waited for a concurrent capture's lock, then re-read the account's *new* balance, but its "credits since cutoff" subquery kept the statement's *old* snapshot, so the new credit was counted as yesterday's. Reproduced deterministically in `TestSettlementIgnoresConcurrentCapture` (₹1,476.40 paid instead of ₹976.40), then fixed by locking in one statement and computing in the next.
6. **2026-10-17 · Settlement dates compared as UTC timestamps.** Found while chasing bug 5: passing an IST-midnight `time.Time` for a `DATE` made Postgres compare it as 18:30 UTC of the day before, so "already paid today" never matched. The unique constraint still blocked double payouts, but every run redid the hold work for every merchant. Fix: pass dates as `YYYY-MM-DD` text.
7. **2026-10-17 · One bad bank answer stalled every poller round.** Caught by a live smoke run: a restarted simulator reissued a bank reference already in the database, that payment's update hit `UNIQUE (bank_ref)`, and the pollers returned on the first error, so the rest of the batch never ran. Fix: per-item errors are collected and the batch continues (`TestPollerSkipsPoisonItem`); the simulator's references now carry a per-run tag.

## Numbers

Measured results only, with the hardware. Hardware: i5-13450HX (10C/16T), 16 GB, Windows 11, Postgres 16 in Docker Desktop (WSL 2), default settings. 15 s runs of `cmd/ledgerbench`.

| Ledger workload | Workers | Throughput | p50 | p99 |
| --- | --- | --- | --- | --- |
| No shared accounts | 1 | 178 /s | 5.5 ms | 8.8 ms |
| No shared accounts | 16 | 1,928 /s | 7.6 ms | 22.4 ms |
| No shared accounts | 32 | 2,141 /s | 12.5 ms | 43.6 ms |
| One hot merchant | 16 | 318 /s | 11.8 ms | 693 ms |
| Capture-shaped (3 shared system accounts) | 16 | 278 /s | 12.5 ms | 576 ms |

Crash test: 60 events, 60 delivered, 30 duplicates, 0 lost (`e2e/kill_test.go`).

Not measured yet: API latency under k6 (phase 6), webhook delivery p99, crash-recovery time from the harness (phase 5).

## Open questions

- Push to GitHub (`github.com/naman777/kosh`) so CI runs? Nothing has been pushed. CI now includes the ~6 s chaos test.
- Phase 6 hot-account fix: shard system accounts (simplest) or batch postings (more interesting to talk about)? This can wait until phase 6.
