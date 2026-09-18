-- A payment is captured at most once: one capture entry per payment.
-- Captured and refunded payments have exactly one posted capture entry; every other payment has none.
-- Every payment the bank authorized (authorized, captured, refunded) has exactly one authorization entry.
SELECT 'capture_count_mismatch' AS problem, p.id::text AS ref,
       format('state=%s capture_entries=%s', p.state, COUNT(e.id)) AS detail
FROM payments p
LEFT JOIN journal_entries e ON e.kind = 'capture' AND e.ref_id = p.id
GROUP BY p.id, p.state
HAVING COUNT(e.id) <> CASE WHEN p.state IN ('captured', 'refunded') THEN 1 ELSE 0 END
UNION ALL
SELECT 'capture_not_posted', e.id::text, format('state=%s', e.state)
FROM journal_entries e
WHERE e.kind = 'capture' AND e.state <> 'posted'
UNION ALL
SELECT 'capture_without_payment', e.id::text, format('ref_id=%s', e.ref_id)
FROM journal_entries e
WHERE e.kind = 'capture' AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.id = e.ref_id)
UNION ALL
SELECT 'capture_amount_mismatch', p.id::text, format('amount=%s debited=%s', p.amount, SUM(ps.amount))
FROM payments p
JOIN journal_entries e ON e.kind = 'capture' AND e.ref_id = p.id
JOIN postings ps ON ps.entry_id = e.id AND ps.side = 'dr'
GROUP BY p.id, p.amount
HAVING SUM(ps.amount) <> p.amount
UNION ALL
SELECT 'authorization_count_mismatch', p.id::text, format('state=%s authorization_entries=%s', p.state, COUNT(e.id))
FROM payments p
LEFT JOIN journal_entries e ON e.kind = 'authorization' AND e.ref_id = p.id
GROUP BY p.id, p.state
HAVING COUNT(e.id) <> CASE WHEN p.state IN ('authorized', 'captured', 'refunded') THEN 1 ELSE 0 END;
