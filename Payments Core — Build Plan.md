# Payments Core — Build Plan

Oct 17, 2026 · @Naman Kundra

## Goal

Build **Kosh**, a payments core in Go. It moves money through a double-entry ledger and stays correct when the bank, the network or its own process fails. The audience is backend hiring managers at Razorpay, CRED, Navi, Zomato and InMobi, where ledgers, webhooks and reconciliation are daily work and standard system design questions.

The plan runs 6 weeks at about 12–15 hours a week, alongside placements. A shareable MVP lands at the end of week 3.

The resume line this project should earn (fill in the numbers after week 6):

> Built Kosh, a payments core in Go and Postgres: a double-entry ledger with two-phase holds, signed at-least-once webhooks, and three-way reconciliation against bank settlement files. 0 invariant violations across 10,000 seeded crash runs; X ledger transfers/s at p99 Y ms.

Kosh (treasury) is a working name; change it freely.

## Scope

Kosh simulates the outside world (the bank, UPI and the merchants), so every failure can be injected on demand.

**In**

- Merchants with API keys, orders and payments (UPI and card, both simulated), plus capture and partial or full refunds
- A double-entry ledger in integer paise, with two-phase holds for refunds and payouts
- Fees and GST posted as separate ledger lines
- Webhooks that are signed, delivered at least once, retried and replayable
- A bank/UPI simulator with timeouts, late callbacks, outages and T+1 settlement files
- T+1 merchant settlement and payouts
- Three-way reconciliation with an exception queue
- A crash-injection simulation harness, load tests and a failure-lab dashboard

**Out** (say so in the README)

- Real bank, UPI or card-network integration, and real money
- Card storage and PCI scope (tokens only)
- KYC, disputes and chargebacks (stretch)
- Multi-currency (INR only) and multi-region

## Architecture

Postgres is the single source of truth. Every money movement, and the event that announces it, commits there in one transaction before anything leaves the system.

&#91;embedded content: Kosh architecture · 9 components\]

The API and workers write payments, ledger entries and outbox rows together. The relay publishes those rows to Kafka, and the dispatcher delivers them to merchants. Separately, the bank simulator's settlement files feed the recon engine, which posts adjusting entries back into the ledger.

| Layer | Choice | Why |
| --- | --- | --- |
| Services | Go | Simple concurrency, fast, and a common choice in Indian fintech backends |
| Source of truth | PostgreSQL 16 | Transactions, row locks, `SKIP LOCKED`, `CHECK` constraints |
| Events | Redpanda (Kafka API), single node | The Kafka model without the ops burden |
| Rate limits | Redis | Per-merchant token buckets only; never money state |
| Observability | Prometheus, Grafana, OpenTelemetry | Latency, lock waits, outbox lag |
| Load tests | k6 | Scriptable scenarios |
| Dashboard | Next.js | Failure lab plus ledger explorer |
| Deploy | Docker Compose on a GCP VM | Same setup as Foreman |

## Core data model

Money is `BIGINT` paise everywhere: no floats, ever. Ledger rows are append-only, and a correction is always a new entry, never an edit.

```sql
CREATE TABLE merchants (
  id UUID PRIMARY KEY, name TEXT NOT NULL,
  fee_bps INT NOT NULL DEFAULT 200,            -- 2.00%
  webhook_url TEXT, webhook_secret TEXT NOT NULL,
  webhook_disabled_at TIMESTAMPTZ);

CREATE TABLE idempotency_keys (
  merchant_id UUID, key TEXT,
  request_hash BYTEA NOT NULL,                 -- same key + different body -> 422
  status TEXT NOT NULL,                        -- in_progress | done
  response JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (merchant_id, key));

CREATE TABLE payments (
  id UUID PRIMARY KEY, merchant_id UUID NOT NULL, order_id UUID NOT NULL,
  amount BIGINT NOT NULL CHECK (amount > 0),
  refunded BIGINT NOT NULL DEFAULT 0 CHECK (refunded <= amount),
  method TEXT NOT NULL,                        -- upi | card
  state TEXT NOT NULL,                         -- created | pending | authorized | captured | failed | refunded
  bank_ref TEXT UNIQUE,                        -- UTR / RRN from the bank
  updated_at TIMESTAMPTZ NOT NULL);

CREATE TABLE accounts (
  id BIGSERIAL PRIMARY KEY,
  code TEXT NOT NULL,                          -- bank_receivable, merchant_payable, fee_revenue, gst_payable ...
  owner_id UUID,                               -- merchant id for merchant_payable
  normal_side TEXT NOT NULL,                   -- debit | credit
  posted_dr BIGINT NOT NULL DEFAULT 0, posted_cr BIGINT NOT NULL DEFAULT 0,
  pending_dr BIGINT NOT NULL DEFAULT 0, pending_cr BIGINT NOT NULL DEFAULT 0,
  flags INT NOT NULL DEFAULT 0);               -- e.g. debits_must_not_exceed_credits

CREATE TABLE journal_entries (
  id UUID PRIMARY KEY,
  kind TEXT NOT NULL,                          -- capture | refund | payout | adjustment
  ref_id UUID NOT NULL,                        -- payment, refund or payout id
  state TEXT NOT NULL,                         -- posted | pending | voided
  expires_at TIMESTAMPTZ,                      -- for pending holds
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (kind, ref_id));                      -- one entry per business event

CREATE TABLE postings (
  entry_id UUID NOT NULL REFERENCES journal_entries,
  account_id BIGINT NOT NULL REFERENCES accounts,
  side TEXT NOT NULL,                          -- dr | cr
  amount BIGINT NOT NULL CHECK (amount > 0));

CREATE TABLE outbox (
  id BIGSERIAL PRIMARY KEY, aggregate_id UUID NOT NULL,
  event_type TEXT NOT NULL, payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ);

CREATE TABLE webhook_deliveries (
  event_id BIGINT, merchant_id UUID, attempt INT NOT NULL,
  next_attempt_at TIMESTAMPTZ, last_status INT, delivered_at TIMESTAMPTZ,
  PRIMARY KEY (event_id, merchant_id));

CREATE TABLE settlement_lines (
  file_id UUID, line_no INT, bank_ref TEXT,
  gross BIGINT, fee BIGINT, tax BIGINT, net BIGINT, value_date DATE,
  PRIMARY KEY (file_id, line_no));

CREATE TABLE recon_results (
  bank_ref TEXT, payment_id UUID,
  status TEXT NOT NULL,                        -- matched | fee_variance | missing_in_bank | missing_in_ledger | duplicate | amount_mismatch
  resolved_by_entry UUID, resolved_at TIMESTAMPTZ);
```

The worked example to put in the README is a ₹1,000 capture at a 2% fee plus 18% GST on that fee.

| Account | Debit (paise) | Credit (paise) |
| --- | --- | --- |
| bank\_receivable | 100,000 |  |
| merchant\_payable (M1) |  | 97,640 |
| fee\_revenue |  | 2,000 |
| gst\_payable |  | 360 |
| **Total** | **100,000** | **100,000** |

The ledger has three rules:

- Postings and the account balance updates commit in one transaction.
- Accounts are locked in ascending id order (`SELECT … FOR UPDATE`) to avoid deadlocks.
- A hold writes `pending_*`, and then post or void moves it exactly once.

## Invariants

These eleven rules are the project's spec. Each one becomes a single SQL query in `invariants/`, run after every simulation step and in CI.

1. Every journal entry balances, and so does the whole ledger: total debits equal total credits.
2. Each account's stored balances equal the sum of its postings, so balances never drift.
3. A payment is captured at most once: one capture entry per payment.
4. `refunded ≤ amount` holds for every payment, including under concurrent refunds.
5. A merchant's payable balance never goes negative, counting pending holds.
6. The same idempotency key with the same body returns the same response. The same key with a different body returns 422 and writes nothing.
7. Every state change writes exactly one outbox event, in the same transaction.
8. Every outbox event eventually reaches each enabled merchant at least once.
9. A pending hold resolves at most once (post or void) and expires by its deadline.
10. Every captured payment either matches a bank settlement line or is in the exception queue by T+2.
11. Posted entries are never updated or deleted.

## Plan, week by week

The plan is six one-week phases starting 19 Oct, with two gates. Gate 1 (end of week 3) is a complete, shareable project on its own; weeks 4–6 add depth and can slip during interview-heavy weeks.

&#91;embedded content: Build roadmap · 6 weeks, 2 gates\]

### Week 1 · Ledger core (Oct 19–25)

- [ ] Repo, Docker Compose (Postgres 16), migrations, CI with `go test`
- [ ] `money` package: paise type, fee and GST math with rounding rules written down
- [ ] Chart of accounts and ledger service: create entry, post, balance check, account flags
- [ ] Invariant checker v0 (invariants 1, 2 and 11)
- [ ] Property test: 1M random transfers keep debits equal to credits

**Done when** the property test and concurrent-transfer tests pass with the race detector on.

### Week 2 · Payments and idempotency (Oct 26–Nov 1)

- [ ] Orders and payments API, payment state machine with guarded transitions (`UPDATE … WHERE state = $expected`)
- [ ] Idempotency-key middleware (in\_progress, done, body-hash mismatch)
- [ ] Bank simulator v1: success, failure, timeout, with configurable latency
- [ ] Capture posts fee and GST lines; partial and full refunds use holds

**Done when** a client retrying any call 5 times never creates a second payment or capture.

### Week 3 · Events and webhooks (Nov 2–8)

- [ ] Transactional outbox, relay with `FOR UPDATE SKIP LOCKED`, Kafka (Redpanda single node)
- [ ] Webhook dispatcher: HMAC-SHA256 over timestamp + body, retries with exponential backoff and jitter, disable after 24 h of failures, replay API
- [ ] Sample merchant receiver that verifies signatures and dedupes by event id
- [ ] Crash tests: kill the relay and dispatcher mid-run, nothing is lost

**Gate 1: shareable MVP.** README, architecture diagram, invariants 1–8 passing, first numbers.

### Week 4 · Bank chaos and settlement (Nov 9–15)

- [ ] Bank simulator v2: late success after our timeout, duplicate callbacks, outages
- [ ] Pending payments plus a status poller with backoff; a late success after `failed` triggers an auto-refund entry
- [ ] T+1 settlement batch per merchant, net of refunds and holds
- [ ] Two-phase payouts: hold, then post on bank confirm or void on failure
- [ ] Bank simulator writes daily settlement files with injected fee, missing, duplicate and mismatch rows

**Done when** invariants 5 and 9 hold under every bank fault.

### Week 5 · Reconciliation and simulation (Nov 16–22)

- [ ] Recon engine: match by `bank_ref`, then amount plus time window; classify into the six statuses
- [ ] Exception queue; resolving one posts an adjusting entry, never an edit
- [ ] Simulation harness: injectable clock and RNG, named crashpoints, workload generator, seed replay
- [ ] 200 seeds per CI run, 10,000 locally

**Done when** 10,000 seeds pass all 11 invariants and recon auto-classifies the injected files.

### Week 6 · Load, observability, launch (Nov 23–29)

- [ ] k6 load tests: steady payments, one hot merchant, webhook fan-out
- [ ] Prometheus and Grafana: latency, lock waits, outbox lag, webhook backlog
- [ ] Next.js failure-lab dashboard; deploy on GCP like Foreman
- [ ] README with numbers and hardware, DESIGN.md with ADRs, 2-minute demo video

**Gate 2: public launch.** Numbers go on the resume and portfolio.

## Testing

Correctness is proven by a crash-injection simulation harness that checks the 11 invariants after every run. Load tests come last.

**Simulation harness**

- Every source of nondeterminism sits behind an interface: `Clock`, `RNG`, `BankClient` and the webhook HTTP sender. A seed fixes the whole run.
- Named crashpoints between steps, for example after the bank call but before the DB commit, after the commit but before publish, and mid-payout. The harness kills the process at a chosen crashpoint, restarts it, and lets recovery run (outbox relay, pending poller, payout resumer).
- A workload generator drives N merchants and M customers through pays, retries with the same key, partial refunds, concurrent refunds and random bank faults.
- The simulated clock fast-forwards, so "merchant down for 26 hours" runs in seconds.
- A failing run prints its seed; `make sim SEED=…` replays it.
- Be honest about scope: the app layer is deterministic, but Postgres runs for real (testcontainers). Say in the README that multi-worker runs replay from a logged schedule rather than being fully deterministic.

**Chaos scenarios**

| Scenario | How it's injected | Expected outcome |
| --- | --- | --- |
| Bank times out, then succeeds late | Callback delayed past the 30 s timeout | Payment goes pending, the poller resolves it, one capture |
| Bank succeeds after we marked the payment failed | Late success callback | Auto-refund entry or exception; money never lost |
| Duplicate bank callbacks | Same callback sent 2–5 times | One capture entry |
| Client retries with the same key | Workload repeats requests | One payment, identical responses |
| Crash after commit, before publish | Crashpoint | Relay publishes on restart; webhook delivered at least once |
| Merchant endpoint down 26 h | Receiver returns 500 | Backoff retries, disabled at 24 h, replay delivers |
| 10 concurrent refunds on one payment | Goroutines racing | `refunded ≤ amount`; extras rejected |
| Settlement file with bad rows | Simulator injects missing, duplicate and mismatched rows | Each row gets the right recon status |
| Payout rejected by the bank | Simulator rejects | Hold voided, balance restored |

**Load tests (k6)**

Run three scenarios: steady create plus capture at fixed req/s, all traffic on one hot merchant, and webhook fan-out. Record p50/p99, throughput and Postgres lock waits, along with the hardware they ran on.

## Metrics to publish

These are targets, not claims. Publish what you actually measure, with the hardware and conditions, the same way your load balancer write-up does.

| Metric | Target | How it's measured |
| --- | --- | --- |
| Invariant violations | 0 across 10,000 seeds | Simulation harness report |
| Payments API latency | p99 under 50 ms at 500 req/s on a laptop | k6 steady scenario |
| Ledger throughput | 2,000+ transfers/s on one Postgres node | k6 plus a batch benchmark |
| Hot-account throughput | Report the baseline, then the improvement | k6 hot-merchant scenario |
| Webhook delivery | p99 under 2 s to healthy endpoints | Delivery timestamps |
| Auto-reconciliation | 98%+ of injected rows classified correctly | Recon report on seeded files |
| Crash recovery | All pending work resumed within 10 s | Harness timings |

## Repo layout and demo

The repo is one Go module with a binary per process, so each piece can be killed on its own in the failure lab.

```text
kosh/
  cmd/          api, worker, relay, dispatcher, banksim, recon, sim
  internal/
    money/        paise type, fee and GST math
    ledger/       accounts, entries, holds, balance checks
    payments/     state machine, idempotency
    outbox/       transactional outbox and relay
    webhooks/     signing, retry schedule, circuit breaker
    bank/         client interface (simulator lives in cmd/banksim)
    settlement/   T+1 batches, payouts
    recon/        matchers, classifiers, exception queue
    clock/ rng/   injectable time and randomness
  invariants/   one SQL file per invariant
  migrations/
  sim/          workload generator, crashpoints, seed replay
  loadtest/     k6 scripts
  dashboard/    Next.js failure lab
  docs/         DESIGN.md, adr/
  docker-compose.yml
```

**Demo plan**

- A live failure lab like MeshPay's, with buttons to kill the relay, black-hole a merchant endpoint and make the bank answer late. Ledger totals stay balanced on screen while it happens.
- README order: a one-paragraph pitch, the architecture diagram, numbers with hardware, the invariants list, then a "where it breaks" section.
- ADRs for the real decisions: Postgres vs TigerBeetle, outbox vs CDC (Debezium), pessimistic vs optimistic locking, and hot accounts.
- A 2-minute video that follows one payment's journal entries through one injected failure.

## Interview talking points and risks

The strongest story is the bugs the harness caught. Keep a log of every one from week 1.

**Talking points**

- Why integer paise and double-entry, instead of a balance column
- Why a transactional outbox instead of dual writes, and what at-least-once means for the merchant
- How a late UPI success is handled: pending, then the poller, then captured or auto-refunded
- How correctness was proven: crashpoints plus invariants, and the first bug the harness found
- Hot accounts: what breaks when one merchant takes 1,000 req/s, and the options (sharded sub-accounts, batched postings)

**Risks**

| Risk | Fallback |
| --- | --- |
| Interviews eat whole weeks | Gate 1 stands on its own; weeks 4–6 slip |
| Running Kafka is a time sink | Redpanda single node, or Postgres LISTEN/NOTIFY first |
| Full determinism with real Postgres is hard | Deterministic app layer plus seed replay; state the scope honestly |
| Scope creep (disputes, multi-currency, AI agent) | Stretch list, only after Gate 2 |
| Hot-account contention drags the numbers down | Publish the baseline anyway; the fix becomes a talking point |

**Stretch, after Gate 2**

- A recon-exception triage agent
- Nightshift investigating Kosh's injected incidents
- A TigerBeetle backend behind the ledger interface, with a benchmark against Postgres
- Disputes and chargebacks
