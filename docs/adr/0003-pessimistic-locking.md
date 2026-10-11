# ADR 0003: Pessimistic row locks in ascending id order

**Status:** accepted, 2026-10-17. Hot system accounts sharded and measured 2026-10-18; one hot merchant is still open.

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

`ledgerbench` on the dev laptop, 2026-10-17 (the first baseline):

| Workload, 16 workers | Throughput | p99 |
| --- | --- | --- |
| No shared accounts | 1,928 /s | 22 ms |
| One hot merchant | 318 /s | 693 ms |
| Capture-shaped (3 shared system accounts) | 278 /s | 576 ms |

A lock is held from the `SELECT … FOR UPDATE` until `COMMIT`, about three round trips. Every capture touches `bank_receivable`, `fee_revenue` and `gst_payable`, so captures are effectively single-file. The p99 is the lock queue.

## Fix: sharded system accounts (phase 6, measured)

Each hot system account (`bank_receivable`, `unallocated_receipts`, `fee_revenue`, `gst_payable`) is split into N interchangeable sub-accounts. The shard is picked from the payment id (FNV-1a), so a payment's authorization, capture, refunds and reconciliation all land on one shard. Reports add the shards up. The count is fixed when a database is created (`KOSH_SYSTEM_SHARDS`, default 16), because changing it would move existing payments to different shards. Locking is unchanged: still one ordered `SELECT … FOR UPDATE`, now over different rows for different payments.

Measured 2026-10-18 on the same laptop, all in one session ([loadtest/RESULTS.md](../../loadtest/RESULTS.md)):

| Workload | 1 shard | 16 shards | Change |
| --- | --- | --- | --- |
| `ledgerbench -mode capture`, 16 workers | 183 /s, p99 734 ms | 1,700 /s, p99 27 ms | 9.3× throughput, 27× lower p99 |
| `ledgerbench -mode capture`, 32 workers | 189 /s, p99 1,308 ms | 2,169 /s, p99 56 ms | 11.5× throughput |
| k6 steady through the API, 100 sales/s | request p99 489 ms, lock-wait p99 246 ms | request p99 109 ms, lock-wait p99 7.6 ms | 4.5× / 32× lower |
| k6 steady, highest sustained rate | 87 sales/s | 197 sales/s (about 300 with a 64-connection pool) | 2.3× or more |

The baseline above was measured on 2026-10-17 with capture debiting `bank_receivable` and no limit check. The 1-shard row here debits `unallocated_receipts` and checks its limit, as production does, and it comes from the same session as the 16-shard row. With 16 shards, captures reach about 80% of the no-contention ceiling (2,112/s), and the API's connection pool becomes the next limit.

## Still open

- **Shorter lock hold.** Move the lock to the last statement before commit, or use a single `UPDATE … WHERE balance check` for single-leg limits.
- **Batched postings for one hot merchant.** A merchant's own payable can't be sharded without breaking "never negative". Collect credits to the same account for a few milliseconds and apply them in one update. One hot merchant tops out near 150 sales/s through the API and 190 postings/s in `ledgerbench`.
