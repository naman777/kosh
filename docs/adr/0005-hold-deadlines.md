# ADR 0005: A hold's deadline is when Kosh stops waiting, not when the bank stops counting

**Status:** accepted, 2026-10-17. Found by the bank chaos test.

## Context

Refunds and payouts are two-phase: Kosh holds the money, asks the bank, then commits or voids the hold. Every hold has a deadline (`expires_at`) so money can't stay reserved forever. The first version enforced it in two places:

- `CommitHold` refused a hold past its deadline (`ErrHoldExpired`).
- The expiry job voided every hold past its deadline.

`TestBankChaos` produced a refund whose bank success arrived after its hold's deadline. The commit was refused. The callback and the worker both failed on it forever, and the expiry job would eventually have voided the hold, recording "refund failed" while the customer already had the money.

## Decision

- **A bank confirmation is a fact.** `ledger.CommitConfirmed` commits a hold regardless of its deadline, as long as it has not been voided. The refund and payout resolvers use it. `CommitHold` keeps the deadline check for commits that no outside party has confirmed.
- **Expiry asks before it voids.** For each expired hold, the expiry job asks the bank. Success commits the hold. Failed, never received, or still pending voids it.
- **The pollers decide well before expiry.** Refunds still pending after `RefundPendingTimeout` (24 h) are failed, while the hold lasts 72 h. Payouts never time out on pending (failing one the bank later completes would pay the merchant twice); only their 7-day hold's expiry ends them.
- **Invariant 9 checks what matters:** no hold still pending past its deadline (plus 10 minutes for the job). It no longer forbids a hold being *posted* late.

## Consequences

- The remaining gap is a bank success that arrives after Kosh has already voided (the poller gave up at 24 h, or expiry found the bank still pending). The ledger and the bank then disagree. That is what reconciliation is for: the settlement file will carry the line, and phase 5's recon classifies it as `missing_in_ledger` and posts an adjusting entry.
- Deadlines now have one meaning across the codebase, "stop waiting and decide", which is easier to reason about than two meanings.
