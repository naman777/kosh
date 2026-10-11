# Load test results

Measured 2026-10-18 on one laptop, with nothing else running.

**Hardware.** Intel Core i5-13450HX (10 cores / 16 threads), 16 GB RAM, Windows 11. Postgres 16 and Redpanda run in Docker Desktop (WSL 2, 16 vCPUs, 7.6 GB) with the compose settings (`max_connections=300`, `shared_buffers=256MB`, `fsync` on). The Kosh binaries (api, worker, relay, dispatcher, banksim, samplemerchant) run natively on the host. k6 0.54.0 runs in Docker on the same machine, so the load generator competes with Kosh for CPU.

**Method.** Each run is a k6 `constant-arrival-rate` scenario lasting 60 s. A *sale* is three API calls: create an order, create a payment (which calls the bank), and capture it. The bank simulator is set to 1–3 ms latency with no failures, so the numbers measure Kosh. Lock-wait and ledger-post p99 come from Prometheus (`kosh_ledger_lock_wait_seconds`, `kosh_ledger_post_seconds`) over each run's window. *Dropped* counts sales k6 could not start because every VU was busy waiting on Kosh, which means the target rate was not sustained. Every run starts from a fresh database.

## Steady traffic, 50 merchants (`steady.js`)

16 shards per hot system account (the default), with the API's default pool of 16 connections:

| Target sales/s | Achieved sales/s | Requests/s | Request p50 | Request p99 | Sale p99 | Errors | Dropped | Lock-wait p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | 99 | 298 | 16 ms | 109 ms | 335 ms | 0% | 0 | 7.6 ms |
| 200 | 197 | 591 | 206 ms | 776 ms | 1.8 s | 0% | 123 | 18 ms |
| 300 | 210 | 632 | 1.5 s | 3.1 s | 6.6 s | 0% | 4,604 | 21 ms |
| 400 | 227 | 682 | 1.9 s | 3.5 s | 7.5 s | 0% | 9,090 | 20 ms |

Above about 200 sales/s, latency climbs while lock waits stay near 20 ms, so row locks are not what limits throughput here. The limit is the API's connection pool: pgx's default of `max(4, NumCPU)` gives 16 connections. With `pool_max_conns=64` in `KOSH_DATABASE_URL`:

| Target sales/s | Achieved sales/s | Requests/s | Request p50 | Request p99 | Sale p99 | Errors | Dropped | Lock-wait p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 300 | 295 | 887 | 64 ms | 386 ms | 891 ms | 0% | 0 | 192 ms |
| 400 | 305 | 916 | 1.3 s | 2.6 s | 5.6 s | 0% | 4,460 | 218 ms |

With the larger pool, Kosh sustains about 300 sales/s (around 900 requests/s) on this laptop. At that rate lock waits become the limit again.

## Sharding before/after (`KOSH_SYSTEM_SHARDS`)

The same steady scenario on a database created with `KOSH_SYSTEM_SHARDS=1`, using the default pool:

| Target sales/s | Shards | Achieved sales/s | Request p50 | Request p99 | Dropped | Lock-wait p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 1 | 98 | 213 ms | 489 ms | 4 | 246 ms |
| 100 | 16 | 99 | 16 ms | 109 ms | 0 | 7.6 ms |
| 200 | 1 | 87 | 2.4 s | 5.6 s | 6,146 | 401 ms |
| 200 | 16 | 197 | 206 ms | 776 ms | 123 | 18 ms |
| 300 | 1 | 86 | 4.0 s | 8.1 s | 11,943 | 443 ms |
| 300 | 16 | 210 | 1.5 s | 3.1 s | 4,604 | 21 ms |

With one shard, every payment queues on the same `bank_receivable`, `unallocated_receipts`, `fee_revenue` and `gst_payable` rows. Throughput tops out near 87 sales/s and falls slightly as more requests pile into the lock queue. Sixteen shards raise the ceiling about 2.5×, until the pool becomes the limit. At 100 sales/s they cut lock-wait p99 32× and request p99 4.5×.

The ledger alone (`ledgerbench`, no HTTP), 15 s runs:

| Workload | Workers | Shards | Throughput | p50 | p99 |
| --- | --- | --- | --- | --- | --- |
| Capture-shaped | 16 | 1 | 183 /s | 17.8 ms | 734 ms |
| Capture-shaped | 16 | 16 | 1,700 /s | 8.0 ms | 27 ms |
| Capture-shaped | 32 | 1 | 189 /s | 18.1 ms | 1,308 ms |
| Capture-shaped | 32 | 16 | 2,169 /s | 11.8 ms | 56 ms |
| No shared accounts (ceiling) | 1 | n/a | 123 /s | 7.5 ms | 15 ms |
| No shared accounts (ceiling) | 16 | n/a | 2,112 /s | 7.1 ms | 16 ms |
| No shared accounts (ceiling) | 32 | n/a | 2,513 /s | 11.3 ms | 36 ms |
| One hot merchant | 16 | n/a | 190 /s | 16.5 ms | 1,149 ms |

With 16 shards, capture-shaped postings reach about 80% of the no-contention ceiling, against 9% with one shard.

These runs differ from the first `ledgerbench` numbers from 2026-10-17: spread with 16 workers was 1,928/s then and is 2,112/s now, while hot merchant was 318/s then and is 190/s now. Run-to-run variance on Docker Desktop is large, so compare rows from the same session. Every row here comes from one session. The capture mode also now funds `unallocated_receipts` first and checks its limit on every capture, as production does. Before, it debited `bank_receivable`, which has no limit.

## One hot merchant (`hot.js`)

Every sale credits the same merchant's payable. That row can't be sharded without breaking "never negative".

| Target sales/s | Achieved sales/s | Request p50 | Request p99 | Errors | Dropped | Lock-wait p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 100 | 14 ms | 31 ms | 0% | 0 | 5.5 ms |
| 200 | 152 | 1.3 s | 3.3 s | 0% | 2,192 | 229 ms |

One merchant tops out near 150 sales/s, queued on its own row. Batched postings are the planned fix (NEXT.md P2).

## Webhook fan-out, 20 merchants (`fanout.js`)

Each sale emits three events, which the relay and one dispatcher deliver to `samplemerchant`'s `/sink`. Delivery latency runs from event creation to a 2xx (`kosh_webhook_delivery_seconds`).

| Target sales/s | Events/s | Achieved sales/s | Request p99 | Errors | Webhook delivery p99 |
| --- | --- | --- | --- | --- | --- |
| 50 | 150 | 50 | 47 ms | 0% | 2.0 s |
| 100 | 300 | 100 | 33 ms | 0% | 29.6 s |

The API keeps up, but one dispatcher falls behind at about 300 events/s and builds a backlog. Partitioning the topic and running several dispatchers is the scale-out path (NEXT.md P2, event order across relays).

## Reproduce

```bash
docker compose --profile obs up -d --wait
go build -o bin/ ./cmd/...
# start banksim, api, worker, relay, dispatcher and samplemerchant as in the README,
# on a fresh database; add KOSH_SYSTEM_SHARDS=1 for the "before" case
loadtest/run.sh steady.js -e RATE=200 -e DURATION=60s
go run ./cmd/ledgerbench -mode capture -shards 1
go run ./cmd/ledgerbench -mode capture -shards 16
```
