-- Merchants, API keys, orders, payments, refunds and idempotency keys.

CREATE TABLE merchants (
  id                  UUID PRIMARY KEY,
  name                TEXT NOT NULL CHECK (name <> ''),
  fee_bps             INT NOT NULL DEFAULT 200 CHECK (fee_bps BETWEEN 0 AND 5000),   -- 2.00%
  webhook_url         TEXT,
  webhook_secret      TEXT NOT NULL,
  webhook_disabled_at TIMESTAMPTZ,
  created_at          TIMESTAMPTZ NOT NULL
);

-- Only a SHA-256 of each key is stored; the key itself is shown once at creation.
CREATE TABLE api_keys (
  key_hash    BYTEA PRIMARY KEY,
  merchant_id UUID NOT NULL REFERENCES merchants (id),
  prefix      TEXT NOT NULL,                       -- first characters, for display
  created_at  TIMESTAMPTZ NOT NULL,
  revoked_at  TIMESTAMPTZ
);

CREATE TABLE orders (
  id          UUID PRIMARY KEY,
  merchant_id UUID NOT NULL REFERENCES merchants (id),
  amount      BIGINT NOT NULL CHECK (amount > 0),
  currency    TEXT NOT NULL DEFAULT 'INR' CHECK (currency = 'INR'),
  receipt     TEXT,
  created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX orders_merchant ON orders (merchant_id, created_at);

CREATE TABLE payments (
  id             UUID PRIMARY KEY,
  merchant_id    UUID NOT NULL REFERENCES merchants (id),
  order_id       UUID NOT NULL REFERENCES orders (id),
  amount         BIGINT NOT NULL CHECK (amount > 0),
  refunded       BIGINT NOT NULL DEFAULT 0 CHECK (refunded >= 0 AND refunded <= amount),
  method         TEXT NOT NULL CHECK (method IN ('upi', 'card')),
  state          TEXT NOT NULL CHECK (state IN ('created', 'pending', 'authorized', 'captured', 'failed', 'refunded')),
  bank_ref       TEXT UNIQUE,                      -- UTR / RRN from the bank
  failure_reason TEXT,
  version        INT NOT NULL DEFAULT 1,           -- +1 on every state change; one outbox event per version
  created_at     TIMESTAMPTZ NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX payments_merchant ON payments (merchant_id, created_at);
-- At most one live payment per order. A failed attempt frees the order for another.
CREATE UNIQUE INDEX payments_one_live_per_order ON payments (order_id) WHERE state <> 'failed';
CREATE INDEX payments_unresolved ON payments (updated_at) WHERE state IN ('created', 'pending');

CREATE TABLE refunds (
  id             UUID PRIMARY KEY,
  payment_id     UUID NOT NULL REFERENCES payments (id),
  merchant_id    UUID NOT NULL REFERENCES merchants (id),
  amount         BIGINT NOT NULL CHECK (amount > 0),
  state          TEXT NOT NULL CHECK (state IN ('pending', 'processed', 'failed')),
  hold_entry_id  UUID NOT NULL REFERENCES journal_entries (id),
  bank_ref       TEXT UNIQUE,
  failure_reason TEXT,
  version        INT NOT NULL DEFAULT 1,
  created_at     TIMESTAMPTZ NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX refunds_payment ON refunds (payment_id);
CREATE INDEX refunds_pending ON refunds (updated_at) WHERE state = 'pending';

-- Idempotency keys follow the "atomic phases" pattern: a request that
-- crashed between phases leaves its recovery_point behind, and a retry with
-- the same key resumes from there once locked_at is stale.
CREATE TABLE idempotency_keys (
  merchant_id    UUID NOT NULL REFERENCES merchants (id),
  key            TEXT NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
  request_method TEXT NOT NULL,
  request_path   TEXT NOT NULL,
  request_hash   BYTEA NOT NULL,                   -- same key + different body → 422
  status         TEXT NOT NULL CHECK (status IN ('in_progress', 'done')),
  recovery_point TEXT NOT NULL DEFAULT 'started',
  resource_id    UUID,                             -- payment, refund or order created by this request
  lease_id       UUID,                             -- fencing token of the request holding the key; NULL when free
  locked_at      TIMESTAMPTZ,                      -- when the lease was taken; stale leases can be taken over
  response_code  INT,
  response       JSON,                             -- JSON, not JSONB, so replays are byte-identical
  created_at     TIMESTAMPTZ NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (merchant_id, key),
  CHECK (status <> 'done' OR (response_code IS NOT NULL AND response IS NOT NULL))
);
