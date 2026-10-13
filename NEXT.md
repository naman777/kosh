# Kosh: what to do next

## Next session: start here

1. **Check the self-maintaining lab on `operator-prod`** against the "done when" column below (deployed 2026-10-18, first nightly reset 2026-10-19 04:00 IST).
2. **Record the 2-minute demo** on the live site, following [docs/DEMO.md](docs/DEMO.md).
3. **Make the repo public**, pin it on GitHub, and add the live link, the video and the resume line ([progress.md § Resume line](progress.md#resume-line)) to the portfolio and resume.

Written 2026-10-18. Phases 0–5 are done, and phase 6 is built, deployed at **https://kosh.namankundra.com** and measured: the 10,000-seed soak passed with 0 failures, and the load numbers are in [loadtest/RESULTS.md](loadtest/RESULTS.md). History is in [progress.md](progress.md) and design in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md). This file is the queue, in priority order.

## P0 · Keep the live demo from hurting its neighbours

`operator-prod` is shared with Foreman, Nightshift and Fathom (2 vCPUs). The lab runs 1 sale/s around the clock, which is about 86,000 payments a day, and it re-runs the invariants every 5 s. Those queries scan whole tables, so both the database and its CPU cost grow every day.

All four are built (`cmd/lab/upkeep.go`) and tested locally on 2026-10-18 with short timers: a killed relay came back on its own, traffic paused with no visitor (0 new payments) and resumed on the next poll, the bank and merchant modes returned to their defaults, and a reset gave a fresh database with a new merchant, webhooks verifying under the new secret and 10/10 invariants. **Deployed to `operator-prod` on 2026-10-18.** Before the deploy, the live lab had 1,318 payments in 17 MB, with `kosh-postgres` at 10.4% CPU. Check the "done when" column after the first night.

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

1. ~~**10,000-seed soak**~~ done 2026-10-18: 10,000 of 10,000 passed with 0 failures.
2. ~~**Load tests**~~ done: [loadtest/RESULTS.md](loadtest/RESULTS.md). The API's pgx pool defaults to 16 connections and caps steady traffic near 220 sales/s; consider setting `pool_max_conns` in the deploy URLs.
3. ~~**Hot-account before/after**~~ done: 183 → 1,700 capture postings/s, p99 734 → 27 ms (ADR 0003, README).
4. ~~**Fill the resume line**~~ done ([progress.md § Resume line](progress.md#resume-line)).
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
| One dispatcher falls behind at ~300 events/s (delivery p99 30 s) | Partition the topic and run several dispatchers in one consumer group | M |
| Concurrency isn't replayable | A multi-goroutine mode for the simulation that logs the interleaving and can replay it | L |

## P3 · Stretch (from the build plan, after launch)

- **TigerBeetle backend** behind the ledger interface, with a benchmark against Postgres on the same workloads (ADR 0001 explains why Postgres came first).
- **Disputes and chargebacks:** a dispute holds the merchant's money; it is won or lost later.
- **Recon-exception triage agent:** reads an exception, the payment's history and the bank line, and drafts a resolution for a human to approve.
- **Nightshift investigating Kosh incidents:** point the Nightshift demo at the failure lab's events.

## Interview prep

- The strongest stories are in [progress.md § Bugs caught by tests](progress.md#bugs-caught-by-tests), especially #4 (a bank confirmation arriving after a hold's deadline), #5 (the READ COMMITTED snapshot race in payouts) and #8 (the harness itself wasn't deterministic).
- Practise the five talking points in the build plan against the live demo: kill the relay, make the bank late, show the ledger staying balanced.
