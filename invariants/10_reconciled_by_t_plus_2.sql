-- Every captured payment either matches a bank settlement line or is in the exception queue by T+2.
-- Once the file for business date D is reconciled (it arrives on D+1, which is T+2 for T = D-1), every
-- payment and refund the bank confirmed before D has a result: settled, or reported missing_in_bank.
-- Also: every line of a reconciled file has exactly one result, and recon_suspense holds exactly the
-- sum of the open exceptions' variances.
WITH latest AS (
  SELECT (max(business_date)::timestamp AT TIME ZONE 'Asia/Kolkata') AS cutoff
  FROM settlement_files WHERE reconciled_at IS NOT NULL
),
items AS (
  SELECT 'payment' AS kind, id, bank_confirmed_at FROM payments WHERE bank_ref IS NOT NULL
  UNION ALL
  SELECT 'refund', id, bank_confirmed_at FROM refunds WHERE bank_ref IS NOT NULL
)
SELECT 'not_reconciled' AS problem, i.kind || ' ' || i.id::text AS ref,
       format('bank confirmed %s, reconciled through %s', i.bank_confirmed_at, latest.cutoff) AS detail
FROM items i, latest
WHERE latest.cutoff IS NOT NULL
  AND i.bank_confirmed_at < latest.cutoff
  AND NOT EXISTS (
    SELECT 1 FROM recon_results r
    WHERE r.item_kind = i.kind AND r.item_id = i.id
      AND r.status IN ('matched', 'fee_variance', 'amount_mismatch', 'missing_in_bank'))
UNION ALL
SELECT 'line_result_count', l.file_id::text || '#' || l.line_no, format('results=%s', COUNT(r.id))
FROM settlement_lines l
JOIN settlement_files f ON f.id = l.file_id AND f.reconciled_at IS NOT NULL
LEFT JOIN recon_results r ON r.file_id = l.file_id AND r.line_no = l.line_no
GROUP BY l.file_id, l.line_no
HAVING COUNT(r.id) <> 1
UNION ALL
SELECT 'suspense_mismatch', 'recon_suspense',
       format('balance=%s open exceptions=%s', a.posted_dr - a.posted_cr, open.total)
FROM accounts a,
     (SELECT COALESCE(SUM(suspense), 0) AS total FROM recon_results WHERE resolved_at IS NULL) open
WHERE a.code = 'recon_suspense' AND a.owner_id IS NULL
  AND a.posted_dr - a.posted_cr <> open.total;
