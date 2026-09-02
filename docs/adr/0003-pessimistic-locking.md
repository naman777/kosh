# ADR 0003: Pessimistic row locks in ascending id order

**Status:** accepted, 2026-10-17. The hot-account fix is still open.

## Context

Posting an entry reads several accounts' balances, checks limit flags, and writes new totals. Two concurrent entries touching the same account must not both pass a limit check that only one of them fits. The options:

1. **Pessimistic:** `SELECT … FOR UPDATE` the accounts, check, write, commit.
2. **Optimistic:** read without locks, write with `WHERE version = $seen`, retry on conflict.
3. **Serializable isolation:** let Postgres detect conflicts and retry on `40001`.
4. **Append-only postings with no stored balance:** compute balances with `SUM`, and enforce limits some other way.

## Decision

Option 1. The ledger locks every account an entry touches with one query, `SELECT … WHERE id = ANY($1) ORDER BY id FOR UPDATE`, in ascending id order. It checks flags in Go, inserts postings, and increments the totals in one `UPDATE … FROM unnest(...)`.

## Why

- **No deadlocks by construction.** Every transaction acquires locks in the same global order. `TestConcurrentTransfers` runs 50 goroutines making crossing transfers and sees no deadlocks.
- **No retry loops.** Under contention, optimistic and serializable approaches turn into retry storms exactly where it hurts (hot accounts). Locks queue instead.
- **Limits stay simple.** Option 4 makes "never negative" a whole-history aggregate on every write, which is far slower than one row read.

## Measured consequence: hot accounts

`ledgerbench` on the dev laptop (README):

| Workload, 16 workers | Throughput | p99 |
| --- | --- | --- |
| No shared accounts | 1,928 /s | 22 ms |
| One hot merchant | 318 /s | 693 ms |
| Capture-shaped (3 shared system accounts) | 278 /s | 576 ms |

A lock is held from the `SELECT … FOR UPDATE` until `COMMIT`, about three round trips. Every capture touches `bank_receivable`, `fee_revenue` and `gst_payable`, so captures are effectively single-file. The p99 is the lock queue.

## Planned fix (phase 6)

- **Sharded system accounts.** Split each system account into N sub-accounts (`bank_receivable#0…#15`) and pick the shard from the payment id. Reports sum the shards. This turns one queue into N.
- **Shorter lock hold.** Move the lock to the last statement before commit, or use a single `UPDATE … WHERE balance check` for single-leg limits.
- **Batched postings for one hot merchant.** Collect credits to the same account for a few milliseconds and apply them in one update.

The baseline above gets published either way; the improvement becomes the talking point.
