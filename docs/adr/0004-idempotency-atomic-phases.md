# ADR 0004: Idempotency with atomic phases and fenced leases

**Status:** accepted, 2026-10-17

## Context

Clients retry. Networks drop responses. The same `POST /v1/payments` can arrive five times, concurrently, or after Kosh crashed halfway through the first attempt, and it must create exactly one payment and one bank charge. The simplest scheme (store the key, run the handler, store the response) breaks if the process dies after the bank call but before the response is saved: the key is stuck `in_progress` forever, or a retry charges twice.

## Decision

Follow Stripe's atomic-phases design:

- A request **acquires** its key, which takes a lease with a random `lease_id` and a timestamp.
- The handler runs as **phases**. Each local phase is one transaction that ends by recording a **recovery point** (`payment_created`, `refund_created`) or the final response. External calls happen between phases and are idempotent on Kosh's own ids.
- A retry that finds a stale lease **takes it over** and resumes from the recovery point.
- Every write after acquiring is **fenced**: `WHERE lease_id = $mine`.

## Alternatives considered

- **A unique constraint on (merchant, key) in the payments table.** This stops duplicate payments but not duplicate bank calls, and it can't replay the original response.
- **Holding a database transaction across the bank call.** Locks stay held for up to 30 s, and a crash still loses the bank's answer.
- **Fencing by `locked_at`.** Rejected after writing the tests: under a fake clock, two takeovers in the same instant share a timestamp. A random lease id cannot collide.

## Consequences

- Responses are stored as `JSON` (not `JSONB`) so replays are byte-identical. `TestRetryStorm` compares bytes.
- A crashed request blocks retries for the lease timeout (60 s). That is the price of never running two copies of the same request at once.
- Requests stuck at a recovery point with no client retry need a background finisher. Phase 4's poller covers payments.
