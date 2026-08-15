# Kosh — Progress

Plan: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) · Spec: [Payments Core — Build Plan.md](Payments%20Core%20—%20Build%20Plan.md)

**Current phase:** Phase 1 · Ledger core
**Last updated:** 2026-10-17

## Status

| Phase | Status |
| --- | --- |
| 0 · Scaffold | in progress |
| 1 · Ledger core | not started |
| 2 · Payments and idempotency | not started |
| 3 · Events and webhooks → Gate 1 | not started |
| 4 · Bank chaos and settlement | not started |
| 5 · Reconciliation and simulation | not started |
| 6 · Load, observability, launch | not started |

## Phase 0 · Scaffold

- [ ] git repo, `go.mod`, `.gitignore`
- [ ] Docker Compose with Postgres 16
- [ ] Embedded migration runner
- [ ] Test database helper (template clone per test)
- [ ] CI workflow (`go vet`, `go test -race`)

## Phase 1 · Ledger core

- [ ] `money` package with written rounding rules
- [ ] Ledger schema and append-only triggers
- [ ] Ledger service: post, holds (commit/void/expire), account flags
- [ ] Invariant checker v0 (1, 2, 11)
- [ ] Property test (1M pure + DB-backed)
- [ ] Concurrent transfer and hold-race tests under `-race`

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
