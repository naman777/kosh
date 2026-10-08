# Kosh: what to do next

## Next session: start here

1. Start Docker Desktop, then bring up the databases:
   ```bash
   docker compose up -d --wait
   ```
2. Finish the simulation soak from where it stopped (seeds 6,101–10,000, about 45 minutes; keep the laptop awake):
   ```bash
   go build -o bin/sim.exe ./cmd/sim && ./bin/sim.exe -start 6101 -seeds 3900 -parallel 8
   ```
3. With nothing else running, do the load tests and the sharding before/after (P1 items 2–3 below), then put the numbers in README.md, ADR 0003 and the resume line, and push.

Written 2026-10-18. Phases 0–5 are done, and phase 6 is built and deployed at **https://kosh.namankundra.com**, with the final numbers still to measure. History is in [progress.md](progress.md) and design in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md). This file is the queue, in priority order.

## P0 · Keep the live demo from hurting its neighbours

`operator-prod` is shared with Foreman, Nightshift and Fathom (2 vCPUs). The lab runs 1 sale/s around the clock, which is about 86,000 payments a day, and it re-runs the invariants every 5 s. Those queries scan whole tables, so both the database and its CPU cost grow every day.

| Task | Why | Done when |
| --- | --- | --- |
| **Traffic only while someone is watching.** The lab stops traffic when no dashboard has polled for 10 minutes, and resumes on the next visit. | Stops growth and CPU when nobody's looking. | Idle overnight: zero new payments. |
| **Nightly reset of `kosh_lab`.** Drop and recreate at 04:00 IST. Only the lab data goes; the merchant is recreated. | Bounded data, invariant checks stay fast. | The database stays under 200 MB. |
| **Invariant checks back off** to every 30 s when no one is watching. | Less CPU on the shared box. | Average CPU of `kosh-postgres` under 5%. |
| **The demo heals itself.** A process killed from the dashboard restarts on its own after 3 minutes, and traffic and bank mode return to defaults after 15. | A visitor can't leave the demo broken for the next one. | Kill everything, wait 3 min: all green. |

## P0 · Server hygiene (yours; I didn't change Foreman)

- **Foreman's Caddy reads a stale copy of its Caddyfile.** The container holds an old copy without Nightshift and Kosh; they work only because the current file was hot-loaded. A restart of that container would drop both. Recreate it once (about a second of blip for every site):
  ```bash
  cd ~/Foreman && sudo docker compose -f docker-compose.prod.yml up -d --force-recreate caddy
  ```
- **Commit the Kosh block in the Foreman repo** (`deploy/Caddyfile`), or a Foreman redeploy that resets the file will drop the route. `deploy/Caddyfile.bak-pre-kosh` can then be deleted.

## P1 · Finish Gate 2 (launch)

1. **10,000-seed soak:** 6,100 passed with 0 failures, then paused. Resume with seeds 6,101–10,000 (see "Next session"), and record the final result in README and progress.md.
2. **Load tests** on the laptop with nothing else running: `loadtest/run.sh steady.js`, `hot.js` and `fanout.js` at several rates. Write `loadtest/RESULTS.md` with p50/p99, throughput, error rate, lock-wait p99 from Grafana, and the hardware.
3. **Hot-account before/after:** `ledgerbench -mode capture -shards 1` against `-shards 16`, plus the k6 steady run with `KOSH_SYSTEM_SHARDS=1` and with `16`. Add the result to ADR 0003 and the README.
4. **Fill the resume line** in progress.md with the measured numbers.
5. **Record the 2-minute demo** on the live site, following [docs/DEMO.md](docs/DEMO.md).
6. **Make the repo public**, pin it on GitHub, and add the live link and video to the portfolio and resume.

## P2 · Product gaps (each is a known "where it breaks" item)

| Gap | Fix | Size |
| --- | --- | --- |
| Uncaptured authorizations hold money forever | Auto-refund authorizations not captured within 5 days, like gateways do | M |
| Failed auto refunds aren't retried | Retry with backoff under a new refund id; raise an exception after N tries | S |
| The exception queue has no owner | Age and SLA per exception, a Grafana alert when the oldest open one is over 24 h | S |
| One hot merchant still serializes on its own payable row | Batched postings: collect credits to one account for ~5 ms and apply them in one update; benchmark against the baseline | M |
| Tables grow without bound | Retention jobs for published outbox rows, delivered webhooks, and idempotency keys (keep 7 days) | S |
| Event order across several relays | Partition the relay by aggregate-id hash so each payment's events stay in one relay | M |
| Concurrency isn't replayable | A multi-goroutine mode for the simulation that logs the interleaving and can replay it | L |

## P3 · Stretch (from the build plan, after launch)

- **TigerBeetle backend** behind the ledger interface, with a benchmark against Postgres on the same workloads (ADR 0001 explains why Postgres came first).
- **Disputes and chargebacks:** a dispute holds the merchant's money; it is won or lost later.
- **Recon-exception triage agent:** reads an exception, the payment's history and the bank line, and drafts a resolution for a human to approve.
- **Nightshift investigating Kosh incidents:** point the Nightshift demo at the failure lab's events.

## Interview prep

- The strongest stories are in [progress.md § Bugs caught by tests](progress.md#bugs-caught-by-tests), especially #4 (a bank confirmation arriving after a hold's deadline), #5 (the READ COMMITTED snapshot race in payouts) and #8 (the harness itself wasn't deterministic).
- Practise the five talking points in the build plan against the live demo: kill the relay, make the bank late, show the ledger staying balanced.
