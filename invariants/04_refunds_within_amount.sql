-- refunded ≤ amount holds for every payment, including under concurrent refunds.
-- Also: refunded equals the sum of its live (pending + processed) merchant refunds, a fully refunded
-- payment's processed refunds cover its amount, and each refund's hold is in the matching state.
-- Auto refunds (returning a late success) never touch refunded: the payment was never captured.
-- Every late success has its auto refund: the customer's money is always on its way back.
SELECT 'refunded_exceeds_amount' AS problem, p.id::text AS ref,
       format('amount=%s refunded=%s', p.amount, p.refunded) AS detail
FROM payments p
WHERE p.refunded > p.amount
UNION ALL
SELECT 'refunded_mismatch', p.id::text,
       format('refunded=%s live_refunds=%s', p.refunded, COALESCE(SUM(r.amount) FILTER (WHERE r.state <> 'failed'), 0))
FROM payments p
LEFT JOIN refunds r ON r.payment_id = p.id AND r.kind = 'merchant'
GROUP BY p.id, p.refunded
HAVING p.refunded <> COALESCE(SUM(r.amount) FILTER (WHERE r.state <> 'failed'), 0)
UNION ALL
SELECT 'refunded_state_mismatch', p.id::text,
       format('state=%s amount=%s processed=%s', p.state, p.amount, COALESCE(SUM(r.amount) FILTER (WHERE r.state = 'processed'), 0))
FROM payments p
LEFT JOIN refunds r ON r.payment_id = p.id AND r.kind = 'merchant'
GROUP BY p.id, p.state, p.amount
HAVING (p.state = 'refunded') <> (p.amount = COALESCE(SUM(r.amount) FILTER (WHERE r.state = 'processed'), 0))
UNION ALL
SELECT 'refund_hold_mismatch', r.id::text, format('refund=%s hold=%s', r.state, e.state)
FROM refunds r
JOIN journal_entries e ON e.id = r.hold_entry_id
WHERE (r.state, e.state) NOT IN (('pending', 'pending'), ('processed', 'posted'), ('failed', 'voided'))
UNION ALL
SELECT 'auto_refund_wrong_payment_state', r.id::text, format('payment state=%s bank_ref=%s', p.state, p.bank_ref)
FROM refunds r
JOIN payments p ON p.id = r.payment_id
WHERE r.kind = 'auto' AND (p.state <> 'failed' OR p.bank_ref IS NULL OR r.amount <> p.amount)
UNION ALL
SELECT 'late_success_without_auto_refund', p.id::text, format('bank_ref=%s', p.bank_ref)
FROM payments p
WHERE p.state = 'failed' AND p.bank_ref IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM refunds r WHERE r.payment_id = p.id AND r.kind = 'auto');
