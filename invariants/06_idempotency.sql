-- The same idempotency key with the same body returns the same response; a different body returns 422 and writes nothing.
-- Replay behaviour is proven by tests; this checks the stored state replays depend on: finished keys keep
-- their response, every payment was created by exactly one key, and no key points at a missing resource.
SELECT 'done_without_response' AS problem, merchant_id::text || '/' || key AS ref, '' AS detail
FROM idempotency_keys
WHERE status = 'done' AND (response IS NULL OR response_code IS NULL)
UNION ALL
SELECT 'payment_key_count', p.id::text, format('keys=%s', COUNT(k.key))
FROM payments p
LEFT JOIN idempotency_keys k
  ON k.resource_id = p.id AND k.request_method = 'POST' AND k.request_path = '/v1/payments'
GROUP BY p.id
HAVING COUNT(k.key) <> 1
UNION ALL
SELECT 'refund_key_count', r.id::text, format('keys=%s', COUNT(k.key))
FROM refunds r
LEFT JOIN idempotency_keys k ON k.resource_id = r.id AND k.request_path LIKE '/v1/payments/%/refunds'
WHERE r.kind = 'merchant'
GROUP BY r.id
HAVING COUNT(k.key) <> 1
UNION ALL
SELECT 'key_resource_missing', k.merchant_id::text || '/' || k.key, format('resource=%s path=%s', k.resource_id, k.request_path)
FROM idempotency_keys k
WHERE k.resource_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM payments WHERE id = k.resource_id)
  AND NOT EXISTS (SELECT 1 FROM refunds WHERE id = k.resource_id)
  AND NOT EXISTS (SELECT 1 FROM payouts WHERE id = k.resource_id)
  AND NOT EXISTS (SELECT 1 FROM orders WHERE id = k.resource_id);
