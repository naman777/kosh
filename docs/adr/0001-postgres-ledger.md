# ADR 0001: The ledger lives in Postgres, not TigerBeetle

**Status:** accepted, 2026-10-17

## Context

Kosh needs a double-entry ledger with two-phase holds, balance limits and strict durability. TigerBeetle is built for exactly that: it has native transfers, pending/post/void, `debits_must_not_exceed_credits`, and throughput in the hundreds of thousands per second. Postgres is a general database that can do it with care.

## Decision

Build the ledger on Postgres 16 tables (`accounts`, `journal_entries`, `postings`), with running totals on the account row and triggers that make history append-only.

## Why

- **One transaction for everything.** A capture changes the payment's state, posts the ledger entry and writes the outbox event. In Postgres that is one `COMMIT`. With TigerBeetle as a second system, Kosh would need its own two-phase protocol between the two stores, which reintroduces the dual-write problem the outbox exists to avoid.
- **Invariants are queries.** Every rule in `invariants/` joins ledger rows with payments, refunds and the outbox. That is trivial when they share a database.
- **Throughput is not the bottleneck yet.** The measured ceiling is about 2,000 transfers/s on a laptop when accounts don't collide (README). The real limit is hot accounts, which TigerBeetle doesn't remove either; it only makes each locked operation cheaper.
- **Interview value.** Explaining row locks, lock order and guarded updates in Postgres is the point of the exercise.

## Consequences

- Kosh owns correctness work TigerBeetle would give for free: lock ordering, hold state machines and balance drift checks. Invariants 1, 2 and 11 exist because of that.
- The ledger API (`Post`, `Hold`, `CommitHold`, `VoidHold`) is narrow enough that a TigerBeetle backend could sit behind it later. That is on the stretch list, with a benchmark against this one.
