-- Pollers, auto refunds, T+1 payouts.

ALTER TABLE payments ADD COLUMN next_poll_at TIMESTAMPTZ;
ALTER TABLE payments ADD COLUMN poll_attempts INT NOT NULL DEFAULT 0;
DROP INDEX payments_unresolved;
CREATE INDEX payments_poll ON payments (next_poll_at) WHERE state IN ('created', 'pending');

-- merchant: refund requested by the merchant, held against merchant_payable.
-- auto: Kosh returning a late success to the customer, held against customer_refund_payable.
ALTER TABLE refunds ADD COLUMN kind TEXT NOT NULL DEFAULT 'merchant' CHECK (kind IN ('merchant', 'auto'));
ALTER TABLE refunds ADD COLUMN next_poll_at TIMESTAMPTZ;
ALTER TABLE refunds ADD COLUMN poll_attempts INT NOT NULL DEFAULT 0;
DROP INDEX refunds_pending;
CREATE INDEX refunds_poll ON refunds (next_poll_at) WHERE state = 'pending';
-- At most one auto refund per payment.
CREATE UNIQUE INDEX refunds_one_auto_per_payment ON refunds (payment_id) WHERE kind = 'auto';

-- One payout per merchant per settlement date: re-running a batch is a no-op.
CREATE TABLE payouts (
  id              UUID PRIMARY KEY,
  merchant_id     UUID NOT NULL REFERENCES merchants (id),
  settlement_date DATE NOT NULL,                     -- business day T whose captures this pays (paid on T+1)
  amount          BIGINT NOT NULL CHECK (amount > 0),
  state           TEXT NOT NULL CHECK (state IN ('pending', 'processed', 'failed')),
  hold_entry_id   UUID NOT NULL REFERENCES journal_entries (id),
  bank_ref        TEXT UNIQUE,
  failure_reason  TEXT,
  version         INT NOT NULL DEFAULT 1,
  next_poll_at    TIMESTAMPTZ,
  poll_attempts   INT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL,
  UNIQUE (merchant_id, settlement_date)
);

CREATE INDEX payouts_poll ON payouts (next_poll_at) WHERE state = 'pending';

ALTER TABLE outbox DROP CONSTRAINT outbox_aggregate_type_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_aggregate_type_check CHECK (aggregate_type IN ('payment', 'refund', 'payout'));
