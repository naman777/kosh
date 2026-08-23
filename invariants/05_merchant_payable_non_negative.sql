-- A merchant's payable balance never goes negative, counting pending holds.
-- posted debits + pending debits ≤ posted credits, and every merchant payable carries the flag that enforces it.
SELECT 'payable_negative' AS problem, a.id::text AS ref,
       format('merchant=%s posted_cr=%s posted_dr=%s pending_dr=%s', a.owner_id, a.posted_cr, a.posted_dr, a.pending_dr) AS detail
FROM accounts a
WHERE a.code = 'merchant_payable' AND a.posted_dr + a.pending_dr > a.posted_cr
UNION ALL
SELECT 'payable_flag_missing', a.id::text, format('merchant=%s flags=%s', a.owner_id, a.flags)
FROM accounts a
WHERE a.code = 'merchant_payable' AND (a.flags & 1) = 0;
