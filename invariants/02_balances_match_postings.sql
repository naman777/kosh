-- Each account's stored balances equal the sum of its postings, so balances never drift.
-- Posted entries count toward posted_*, pending holds toward pending_*, voided holds toward nothing.
WITH sums AS (
  SELECT p.account_id,
         COALESCE(SUM(p.amount) FILTER (WHERE e.state = 'posted'  AND p.side = 'dr'), 0) AS posted_dr,
         COALESCE(SUM(p.amount) FILTER (WHERE e.state = 'posted'  AND p.side = 'cr'), 0) AS posted_cr,
         COALESCE(SUM(p.amount) FILTER (WHERE e.state = 'pending' AND p.side = 'dr'), 0) AS pending_dr,
         COALESCE(SUM(p.amount) FILTER (WHERE e.state = 'pending' AND p.side = 'cr'), 0) AS pending_cr
  FROM postings p
  JOIN journal_entries e ON e.id = p.entry_id
  GROUP BY p.account_id
)
SELECT a.id AS account_id, a.code,
       a.posted_dr, COALESCE(s.posted_dr, 0) AS sum_posted_dr,
       a.posted_cr, COALESCE(s.posted_cr, 0) AS sum_posted_cr,
       a.pending_dr, COALESCE(s.pending_dr, 0) AS sum_pending_dr,
       a.pending_cr, COALESCE(s.pending_cr, 0) AS sum_pending_cr
FROM accounts a
LEFT JOIN sums s ON s.account_id = a.id
WHERE (a.posted_dr, a.posted_cr, a.pending_dr, a.pending_cr)
      IS DISTINCT FROM
      (COALESCE(s.posted_dr, 0), COALESCE(s.posted_cr, 0), COALESCE(s.pending_dr, 0), COALESCE(s.pending_cr, 0));
