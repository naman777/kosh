-- Every state change writes exactly one outbox event, in the same transaction.
-- A payment, refund or payout at version N has events for versions 1..N and no others (the unique
-- (aggregate_id, aggregate_version) key rules out duplicates), and its latest event names its current state.
WITH aggregates AS (
  SELECT 'payment' AS kind, id, merchant_id, version, 'payment.' || state AS want_type FROM payments
  UNION ALL
  SELECT 'refund', id, merchant_id, version,
         CASE WHEN version = 1 THEN 'refund.created' ELSE 'refund.' || state END
  FROM refunds
  UNION ALL
  SELECT 'payout', id, merchant_id, version,
         CASE WHEN version = 1 THEN 'payout.created' ELSE 'payout.' || state END
  FROM payouts
),
events AS (
  SELECT aggregate_id, COUNT(*) AS n, MAX(aggregate_version) AS max_version
  FROM outbox GROUP BY aggregate_id
)
SELECT 'event_count_mismatch' AS problem, a.id::text AS ref,
       format('%s version=%s events=%s max_event_version=%s', a.kind, a.version, COALESCE(e.n, 0), e.max_version) AS detail
FROM aggregates a
LEFT JOIN events e ON e.aggregate_id = a.id
WHERE COALESCE(e.n, 0) <> a.version OR e.max_version IS DISTINCT FROM a.version
UNION ALL
SELECT 'latest_event_wrong_type', a.id::text, format('want=%s got=%s', a.want_type, o.event_type)
FROM aggregates a
JOIN outbox o ON o.aggregate_id = a.id AND o.aggregate_version = a.version
WHERE o.event_type <> a.want_type
UNION ALL
SELECT 'event_merchant_mismatch', o.id::text, format('event merchant=%s owner=%s', o.merchant_id, a.merchant_id)
FROM outbox o
JOIN aggregates a ON a.id = o.aggregate_id
WHERE o.merchant_id <> a.merchant_id
UNION ALL
SELECT 'event_without_aggregate', o.id::text, format('aggregate=%s', o.aggregate_id)
FROM outbox o
WHERE NOT EXISTS (SELECT 1 FROM aggregates a WHERE a.id = o.aggregate_id);
