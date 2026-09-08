-- A pending hold resolves at most once (post or void) and expires by its deadline.
-- The journal_entries trigger allows only pending → posted | voided, once. This checks the deadline
-- (with 10 minutes for the expiry job to run), that nothing was posted after its deadline, and that
-- every refund and payout hold is in the state its business row says.
-- "Now" is kosh.now when set (tests on a fake clock), else the database clock.
WITH clock AS (
  SELECT COALESCE(NULLIF(current_setting('kosh.now', true), '')::timestamptz, now()) AS now
)
SELECT 'hold_past_deadline' AS problem, e.id::text AS ref,
       format('kind=%s expires_at=%s', e.kind, e.expires_at) AS detail
FROM journal_entries e, clock
WHERE e.state = 'pending' AND e.expires_at < clock.now - interval '10 minutes'
UNION ALL
SELECT 'hold_posted_after_deadline', e.id::text, format('expires_at=%s resolved_at=%s', e.expires_at, e.resolved_at)
FROM journal_entries e
WHERE e.state = 'posted' AND e.expires_at IS NOT NULL AND e.resolved_at > e.expires_at
UNION ALL
SELECT 'payout_hold_mismatch', p.id::text, format('payout=%s hold=%s', p.state, e.state)
FROM payouts p
JOIN journal_entries e ON e.id = p.hold_entry_id
WHERE (p.state, e.state) NOT IN (('pending', 'pending'), ('processed', 'posted'), ('failed', 'voided'))
UNION ALL
SELECT 'payout_amount_mismatch', p.id::text, format('payout=%s held=%s', p.amount, SUM(ps.amount))
FROM payouts p
JOIN postings ps ON ps.entry_id = p.hold_entry_id AND ps.side = 'dr'
GROUP BY p.id, p.amount
HAVING SUM(ps.amount) <> p.amount;
