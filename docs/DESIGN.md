# Kosh design

This document covers how Kosh stays correct. The README covers what it does, and the ADRs in [adr/](adr) record the decisions with their alternatives.

## Principles

1. **One source of truth.** Postgres holds every money movement and every event. Nothing else (Kafka, Redis, a merchant) is ever authoritative.
2. **One transaction per fact.** A payment's state, its ledger entry and the outbox event announcing it commit together or not at all.
3. **Unknown is not failure.** A timed-out bank call means the money may have moved. Kosh records `pending` and asks again; it never guesses `failed`.
4. **Append, don't edit.** Ledger rows are immutable, and triggers enforce that. Corrections are new entries.
5. **Every rule is a query.** If a property matters, it is written as SQL in `invariants/` and checked after every test.

## Data model

| Table | Holds | Written by |
| --- | --- | --- |
| `accounts` | chart of accounts with running posted/pending totals and limit flags | ledger |
| `journal_entries`, `postings` | the ledger; append-only, `UNIQUE (kind, ref_id)` | ledger |
| `merchants`, `api_keys` | merchants, fee rates, webhook config; SHA-256 of API keys | admin API |
| `orders`, `payments`, `refunds` | the payment lifecycle, with a `version` per row | payments |
| `idempotency_keys` | leases, recovery points and stored responses | idempotency |
| `outbox` | one row per state change, `UNIQUE (aggregate_id, aggregate_version)` | payments |
| `webhook_deliveries` | one row per (event, merchant), with retry state | dispatcher |
| `payouts` | one per merchant per settlement date, with its hold | worker |

## Money flow

| Event | Debit | Credit | Entry |
| --- | --- | --- | --- |
| Capture | `bank_receivable` amount | `merchant_payable` net, `fee_revenue` fee, `gst_payable` GST | posted |
| Refund requested | `merchant_payable` amount | `bank_receivable` amount | **pending** hold |
| Refund succeeded | (hold moves pending → posted) | | posted |
| Refund failed or expired | (hold voided, pending totals released) | | voided |
| Late success (bank succeeded after Kosh failed the payment) | `bank_receivable` | `customer_refund_payable` | posted |
| Auto refund of a late success | `customer_refund_payable` | `bank_receivable` | **pending** hold, then posted |
| Payout initiated (T+1) | `merchant_payable` eligible amount | `payout_clearing` | **pending** hold |
| Payout confirmed by the bank | (hold posted) and `payout_clearing` | `bank_settlement` | posted |
| Payout rejected | (hold voided; tomorrow's batch pays it) | | voided |

`merchant_payable` carries `DebitsMustNotExceedCredits`, which is checked against posted plus pending debits. A hold reserves money, and a refund the merchant can't cover is refused before the bank is ever called.

## Payment lifecycle

```mermaid
stateDiagram-v2
    [*] --> created
    created --> authorized: bank success
    created --> failed: bank declined
    created --> pending: bank timeout
    pending --> authorized: callback / poller
    pending --> failed: bank declined, or 30 min timeout
    authorized --> captured: capture
    authorized --> failed: auth expired
    captured --> refunded: refunds cover the amount
    failed --> failed: late success → auto refund
```

Every arrow is one call to `transition(tx, id, from, to)`:

```sql
UPDATE payments SET state = $to, version = version + 1, …
WHERE id = $id AND state = $from
RETURNING …
```

Zero rows means a lost race or an illegal request, and the caller gets a typed error. The same function writes the outbox event for the new version, so invariant 7 holds by construction. Invariant 3 is held twice: once by the guard and once by `journal_entries UNIQUE (kind, ref_id)`.

## Idempotency: atomic phases

```text
Acquire(key)          INSERT … ON CONFLICT; compare request hash; take a lease (random lease_id)
  Phase A  (tx)       lock order, insert payment 'created', outbox event, Advance(recovery_point='payment_created')
  Phase B             bank.Authorize(payment_id)        ← idempotent at the bank
  Phase C  (tx)       transition to authorized/failed/pending, outbox event, Finish(response bytes)
```

| What a retry finds | Result |
| --- | --- |
| `done` | the stored response, byte for byte (the column is `JSON`, not `JSONB`, so key order survives) |
| `in_progress`, lease fresh | 409 `idempotency_key_in_use` |
| `in_progress`, lease older than 60 s | take over the lease, resume at the recovery point |
| different request hash | 422 `idempotency_key_reused`, nothing written |

Every write after Acquire includes `WHERE lease_id = $mine`. A request that stalled past its lease and was taken over cannot commit later. Deterministic 4xx errors are stored and replayed. 5xx errors release the lease so a retry resumes at once.

## Bank answers

The same answer can arrive three times, in any order: phase C of the request, a signed callback (`POST /bank/callbacks`, sent late and up to N times), and the worker's status poll. All three call one resolver per aggregate (`applyPaymentResult`, `resolveRefund`, `resolvePayout`). Each locks the row and decides from its *current* state, so a duplicate or stale answer is a no-op.

| Payment is | Bank says success | Bank says failed | Bank says pending / unknown |
| --- | --- | --- | --- |
| `created` / `pending` | → `authorized` | → `failed` | → `pending`, poll again |
| `failed` | late success → auto refund | nothing | nothing |
| `authorized` and later | nothing | nothing | nothing |

The pollers claim due rows with `FOR UPDATE SKIP LOCKED`, push their next poll out by an exponential backoff (5 s to 5 min), and ask `bank.Status`. A request the bank never received is failed (payments) or submitted now (refunds and payouts, which Kosh owes). A row that can't be applied is reported and retried, and the rest of the batch carries on.

## Settlement and payouts

For business day D (IST), the worker pays each merchant:

```text
eligible = posted credits − posted debits − pending debits − credits posted since the start of D+1
```

That nets out refunds and open holds, and leaves D+1's sales for tomorrow. The account row is locked in its own statement *before* this is computed: under READ COMMITTED, a `SELECT … FOR UPDATE` that waits on a lock re-reads the row but keeps its subqueries' old snapshot, which let a concurrent capture leak into yesterday's payout (`TestSettlementIgnoresConcurrentCapture`).

`UNIQUE (merchant_id, settlement_date)` makes the batch safe to repeat. Payouts run in phases like payments, and a crash after the hold leaves a `pending` payout that the poller submits. Holds and the bank's late confirmations follow [ADR 0005](adr/0005-hold-deadlines.md).

The bank simulator publishes a settlement file per day (`GET /v1/settlements/{date}`) with seeded faults, ready for phase 5's reconciliation.

## Events and webhooks

```text
payments tx ──▶ outbox row
relay:       SELECT … WHERE published_at IS NULL FOR UPDATE SKIP LOCKED
             → Kafka (acks=all) → crashpoint relay.after_publish → UPDATE published_at
dispatcher:  consume → INSERT webhook_deliveries ON CONFLICT DO NOTHING
             → crashpoint dispatcher.after_ingest → commit offsets
             claim due rows (lease = 2 × send timeout) → POST → record result
```

Each crashpoint marks a place where dying creates a duplicate and never a loss:

| Crash at | What happens on restart | Tested by |
| --- | --- | --- |
| `relay.after_publish` | batch published again; dispatcher ignores known events | `TestRelayCrashAfterPublish`, `TestKillRelayAndDispatcher` |
| `dispatcher.after_ingest` | consumer re-reads from the last commit; inserts are no-ops | `TestDispatcherCrashAfterIngest` |
| `dispatcher.after_send` | lease expires, merchant gets the event again | `TestDispatcherCrashAfterSend`, `TestKillRelayAndDispatcher` |
| `payments.create.after_insert` / `after_bank` | retry resumes from the recovery point; bank dedupes by payment id | `TestCrashBetweenPhases` |
| `refunds.create.after_bank` | hold stays pending until the retry commits it | `TestCrashDuringRefund` |
| `refunds.create.after_reserve` | the refund poller submits it to the bank | `TestRefundPolling` |
| `payouts.after_hold` | the payout poller submits it | `TestPayoutCrashResumes` |

Retries back off exponentially from 10 s to a 1 h cap. Each delay is half fixed and half random ("equal jitter"), so retries spread out without collapsing to zero. A merchant with no successful delivery for 24 h gets `webhook_disabled_at` and receives nothing until it calls `POST /v1/webhooks/replay`. `TestMerchantDown26Hours` runs that whole story on a fake clock in a few seconds.

## Testing approach

- Every test gets its own Postgres database, cloned from a migrated template in milliseconds.
- `invariants.CheckAt(now)` hands the test's fake clock to the SQL (`kosh.now`), so time-based rules don't depend on the real date.
- `TestBankChaos` runs the API, the worker and a misbehaving bank together over HTTP and checks every invariant after the system drains.
- Time, ids, randomness, the bank and the webhook sender all sit behind interfaces, so tests run on a fake clock with scripted bank outcomes.
- Crashpoints are compiled into the binaries. `KOSH_CRASHPOINT=relay.after_publish` makes the process exit with status 137 at that line. In-process tests arm them with a panic instead.
- Each database test ends with `invariants.Check`, which runs all the SQL rules.
- The e2e test builds the real binaries, runs them against Redpanda, kills them, and checks that nothing was lost.

## Known gaps (after phase 4)

- No reconciliation yet. Settlement files are produced but not read. A bank success after Kosh gave up, or a failed auto refund, is not surfaced until phase 5's exception queue. `bank_settlement` records payouts but not incoming cash.
- Invariant 10 comes with recon.
- The hot system accounts are measured but not yet fixed (ADR 0003).
