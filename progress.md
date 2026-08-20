# Kosh — Progress

Plan: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) · Spec: [Payments Core — Build Plan.md](Payments%20Core%20—%20Build%20Plan.md)

**Current phase:** Phase 2 · Payments and idempotency
**Last updated:** 2026-10-17

## Status

| Phase | Status |
| --- | --- |
| 0 · Scaffold | done |
| 1 · Ledger core | done (2026-10-17) |
| 2 · Payments and idempotency | in progress |
| 3 · Events and webhooks → Gate 1 | not started |
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

- [ ] Merchants, API keys, orders, payments schema
- [ ] Payment state machine with guarded transitions
- [ ] Idempotency middleware (atomic phases, recovery points)
- [ ] Bank simulator v1 + `bank.Fake`
- [ ] Capture with fee and GST lines
- [ ] Partial and full refunds with holds
- [ ] Invariants 3–6, retry-storm and concurrent-refund tests

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

## Bugs caught by tests

The interview story. Every bug a test, invariant or the harness caught, with the date and the fix.

_None yet._

## Numbers

Measured results only, with the hardware.

_None yet._

## Open questions

_None right now._
