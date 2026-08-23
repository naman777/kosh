-- refunded ≤ amount holds for every payment, including under concurrent refunds.
-- Also: refunded equals the sum of its live (pending + processed) refunds, a fully refunded payment's
-- processed refunds cover its amount, and each refund's hold is in the matching state.
SELECT 'refunded_exceeds_amount' AS problem, p.id::text AS ref,
       format('amount=%s refunded=%s', p.amount, p.refunded) AS detail
FROM payments p
WHERE p.refunded > p.amount
UNION ALL
SELECT 'refunded_mismatch', p.id::text,
       format('refunded=%s live_refunds=%s', p.refunded, COALESCE(SUM(r.amount) FILTER (WHERE r.state <> 'failed'), 0))
FROM payments p
LEFT JOIN refunds r ON r.payment_id = p.id
GROUP BY p.id, p.refunded
HAVING p.refunded <> COALESCE(SUM(r.amount) FILTER (WHERE r.state <> 'failed'), 0)
UNION ALL
SELECT 'refunded_state_mismatch', p.id::text,
       format('state=%s amount=%s processed=%s', p.state, p.amount, COALESCE(SUM(r.amount) FILTER (WHERE r.state = 'processed'), 0))
FROM payments p
LEFT JOIN refunds r ON r.payment_id = p.id
GROUP BY p.id, p.state, p.amount
HAVING (p.state = 'refunded') <> (p.amount = COALESCE(SUM(r.amount) FILTER (WHERE r.state = 'processed'), 0))
UNION ALL
SELECT 'refund_hold_mismatch', r.id::text, format('refund=%s hold=%s', r.state, e.state)
FROM refunds r
JOIN journal_entries e ON e.id = r.hold_entry_id
WHERE (r.state, e.state) NOT IN (('pending', 'pending'), ('processed', 'posted'), ('failed', 'voided'));
