-- Every journal entry balances, and so does the whole ledger: total debits equal total credits.
-- Returns one row per violation. Empty result = invariant holds.
WITH per_entry AS (
  SELECT e.id,
         COUNT(p.id) AS n,
         COALESCE(SUM(p.amount) FILTER (WHERE p.side = 'dr'), 0) AS dr,
         COALESCE(SUM(p.amount) FILTER (WHERE p.side = 'cr'), 0) AS cr
  FROM journal_entries e
  LEFT JOIN postings p ON p.entry_id = e.id
  GROUP BY e.id
)
SELECT 'entry_unbalanced' AS problem, id::text AS ref, dr, cr
FROM per_entry WHERE dr <> cr
UNION ALL
SELECT 'entry_too_few_postings', id::text, dr, cr
FROM per_entry WHERE n < 2
UNION ALL
SELECT 'ledger_posted_unbalanced', NULL, SUM(posted_dr), SUM(posted_cr)
FROM accounts HAVING SUM(posted_dr) <> SUM(posted_cr)
UNION ALL
SELECT 'ledger_pending_unbalanced', NULL, SUM(pending_dr), SUM(pending_cr)
FROM accounts HAVING SUM(pending_dr) <> SUM(pending_cr);
