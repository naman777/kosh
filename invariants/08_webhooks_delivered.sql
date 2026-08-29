-- Every outbox event eventually reaches each enabled merchant at least once.
-- "Eventually" means once the system is quiescent: run this after the relay and dispatcher have drained.
-- Counts merchants that have a webhook URL and are not disabled.
SELECT 'event_not_delivered' AS problem, o.id::text AS ref,
       format('merchant=%s type=%s published=%s attempts=%s last_status=%s',
              o.merchant_id, o.event_type, o.published_at IS NOT NULL, d.attempt, d.last_status) AS detail
FROM outbox o
JOIN merchants m ON m.id = o.merchant_id
LEFT JOIN webhook_deliveries d ON d.event_id = o.id AND d.merchant_id = o.merchant_id
WHERE m.webhook_url IS NOT NULL
  AND m.webhook_disabled_at IS NULL
  AND d.delivered_at IS NULL
UNION ALL
SELECT 'delivery_to_wrong_merchant', d.event_id::text, format('delivery merchant=%s event merchant=%s', d.merchant_id, o.merchant_id)
FROM webhook_deliveries d
JOIN outbox o ON o.id = d.event_id
WHERE d.merchant_id <> o.merchant_id;
