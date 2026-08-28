-- Transactional outbox and webhook deliveries.
--
-- Every state change inserts one outbox row in the same transaction as the
-- change itself (invariant 7). The relay publishes unpublished rows to
-- Kafka; the dispatcher turns each event into one delivery per merchant and
-- retries until the merchant acknowledges it (invariant 8).

CREATE TABLE outbox (
  id                BIGSERIAL PRIMARY KEY,                -- also the public event id
  aggregate_type    TEXT NOT NULL CHECK (aggregate_type IN ('payment', 'refund')),
  aggregate_id      UUID NOT NULL,
  aggregate_version INT NOT NULL CHECK (aggregate_version > 0),
  merchant_id       UUID NOT NULL REFERENCES merchants (id),
  event_type        TEXT NOT NULL,                        -- payment.captured, refund.processed ...
  payload           JSON NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL,
  published_at      TIMESTAMPTZ,
  UNIQUE (aggregate_id, aggregate_version)                -- exactly one event per version
);

CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX outbox_merchant ON outbox (merchant_id, id);

CREATE TABLE webhook_deliveries (
  event_id        BIGINT NOT NULL REFERENCES outbox (id),
  merchant_id     UUID NOT NULL REFERENCES merchants (id),
  attempt         INT NOT NULL DEFAULT 0,                 -- attempts started so far
  next_attempt_at TIMESTAMPTZ,                            -- NULL once delivered
  last_status     INT,                                    -- HTTP status of the last attempt
  last_error      TEXT,
  delivered_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, merchant_id)
);

CREATE INDEX webhook_deliveries_due ON webhook_deliveries (next_attempt_at) WHERE delivered_at IS NULL;

-- When the merchant's endpoint started failing without a success since.
-- After 24 hours of that, webhook_disabled_at is set and deliveries pause
-- until the merchant calls the replay API.
ALTER TABLE merchants ADD COLUMN webhook_failing_since TIMESTAMPTZ;
