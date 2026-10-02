# Two-minute demo: one payment through one failure

The build plan's brief: follow one payment's journal entries through one injected failure, with the failure lab on screen. The script is about 260 spoken words, so speak at a normal pace.

**Setup before recording:** `docker compose up -d --wait`, then `go run ./cmd/lab` and `npm --prefix dashboard run dev`. Open the dashboard (localhost:3005) at 1280 px wide, set traffic to 5/s, set the bank to Healthy, and open a terminal with `psql` on `kosh_lab` beside it.

| Time | On screen | Say |
| --- | --- | --- |
| 0:00 | Dashboard, everything green | "This is Kosh, a payments core I built in Go and Postgres. Every rupee moves through a double-entry ledger, and this lab lets me break it while customers keep paying." |
| 0:12 | Point at the two header pills | "Debits equal credits, to the paisa, and ten invariants written as SQL are re-checked every five seconds." |
| 0:20 | Terminal: `SELECT kind, side, amount FROM postings JOIN journal_entries e ON e.id = entry_id WHERE ref_id = '<a payment id>'` | "Here's one ₹1,000 payment. Authorization: the bank owes us, the money is unallocated. Capture: 976.40 to the merchant, 20 rupees fee, 3.60 GST. Every entry balances." |
| 0:40 | Click Bank → Late | "Now the bank goes slow. Half its answers come 15 seconds late, and some replies time out." |
| 0:50 | Payments bar shows pending, then captured | "A timeout means unknown, not failed. The payment waits as pending, and the worker's poller and the bank's late callback both resolve it, through one code path." |
| 1:05 | Click Kill on relay; the Events tile climbs | "I'll kill the relay with SIGKILL. Events can't reach Kafka, so they pile up, safely in Postgres, written in the same transaction as the money." |
| 1:20 | Kill dispatcher too, set merchant endpoint to 500s | "And the webhook dispatcher, and the merchant's server starts failing." |
| 1:30 | Start relay and dispatcher, set merchant back to Up; both tiles drain | "Bring them back. Everything drains: at-least-once delivery, signed webhooks, the merchant dedupes by event id." |
| 1:45 | Pills still green; cut to terminal `go run ./cmd/sim -seeds 200` summary | "Through all of that the ledger never went out of balance. The same rules are checked after every step of 10,000 seeded simulation runs, with crashes at eleven named points, and every failing seed replays exactly." |
| 1:58 | Repo URL | "The code, the numbers and the bugs it caught are on GitHub." |

**Shots to avoid:** API keys (the lab never shows them), and the Grafana admin page.
