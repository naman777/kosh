# Kosh — Progress

Plan: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) · Spec: [Payments Core — Build Plan.md](Payments%20Core%20—%20Build%20Plan.md)

**Current phase:** Phase 3 · Events and webhooks
**Last updated:** 2026-10-17

## Status

| Phase | Status |
| --- | --- |
| 0 · Scaffold | done |
| 1 · Ledger core | done (2026-10-17) |
| 2 · Payments and idempotency | done (2026-10-17) |
| 3 · Events and webhooks → Gate 1 | in progress |
| 4 · Bank chaos and settlement | not started |
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

- [ ] Transactional outbox + relay (`SKIP LOCKED`) + Redpanda
- [ ] Webhook dispatcher: signing, backoff with jitter, 24 h disable, replay API
- [ ] Sample merchant receiver
- [ ] Crashpoints and crash tests for relay and dispatcher
- [ ] Invariants 7–8
- [ ] README, architecture diagram, first ADRs, first numbers

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

## Bugs caught by tests

The interview story. Every bug a test, invariant or the harness caught, with the date and the fix.

_None yet._

## Numbers

Measured results only, with the hardware.

_None yet._

## Open questions

_None right now._
