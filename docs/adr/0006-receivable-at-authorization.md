# ADR 0006: Recognize the bank receivable at authorization, not capture

**Status:** accepted, 2026-10-18. Found while designing reconciliation.

## Context

Until phase 5, the capture entry did everything: Dr `bank_receivable`, Cr merchant net, fee and GST. An authorized payment that was never captured had no ledger entry at all.

The bank doesn't see it that way. For UPI the money leaves the customer at authorization, and the bank's settlement file lists every successful authorization, captured or not. Reconciling such a line would credit a receivable that was never debited: either the receivable goes negative, or every uncaptured payment becomes a `missing_in_ledger` exception, which is noise that hides real problems.

## Decision

Split the money's arrival from its allocation:

| Event | Debit | Credit |
| --- | --- | --- |
| Authorization (bank success) | `bank_receivable` | `unallocated_receipts` |
| Capture | `unallocated_receipts` | merchant net, `fee_revenue`, `gst_payable` |

`unallocated_receipts` is a liability with `DebitsMustNotExceedCredits`: money Kosh holds for customers that belongs to no merchant yet. Invariant 3 now also checks one authorization entry per authorized, captured or refunded payment.

## Consequences

- Reconciliation can settle every line for a payment the bank collected, so recon only raises exceptions for real disagreements.
- The ledger says, at any moment, how much customer money is unallocated, which is a number finance and regulators both ask for.
- Uncaptured authorizations stay in `unallocated_receipts` forever. Gateways auto-refund them after a few days; Kosh doesn't yet. That is listed under "Where it breaks".
- Capture now locks `unallocated_receipts` instead of `bank_receivable`, so it still touches three shared system accounts. The hot-account numbers in ADR 0003 are unchanged in shape.
