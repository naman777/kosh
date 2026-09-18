-- Reconciliation: the bank's settlement files, one result per line (and per
-- ledger item the bank never settled), and the exception queue.

-- When Kosh learned the bank succeeded: the "T" in T+2.
ALTER TABLE payments ADD COLUMN bank_confirmed_at TIMESTAMPTZ;
ALTER TABLE refunds ADD COLUMN bank_confirmed_at TIMESTAMPTZ;
UPDATE payments SET bank_confirmed_at = updated_at WHERE bank_ref IS NOT NULL;
UPDATE refunds SET bank_confirmed_at = updated_at WHERE bank_ref IS NOT NULL;
CREATE INDEX payments_bank_confirmed ON payments (bank_confirmed_at) WHERE bank_ref IS NOT NULL;
CREATE INDEX refunds_bank_confirmed ON refunds (bank_confirmed_at) WHERE bank_ref IS NOT NULL;

CREATE TABLE settlement_files (
  id               UUID PRIMARY KEY,
  business_date    DATE NOT NULL UNIQUE,              -- the bank's day of activity
  value_date       DATE NOT NULL,                     -- when the cash lands (T+1)
  checksum         BYTEA NOT NULL,                    -- SHA-256 of the file as received
  line_count       INT NOT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL,
  reconciled_at    TIMESTAMPTZ,
  settlement_entry UUID REFERENCES journal_entries (id) -- the one ledger entry this file produced
);

CREATE TABLE settlement_lines (
  file_id    UUID NOT NULL REFERENCES settlement_files (id),
  line_no    INT NOT NULL,
  bank_ref   TEXT NOT NULL,
  reference  TEXT NOT NULL,                           -- Kosh's id as the bank echoed it
  gross      BIGINT NOT NULL,                         -- payments positive, refunds negative
  fee        BIGINT NOT NULL,
  tax        BIGINT NOT NULL,
  net        BIGINT NOT NULL,
  value_date DATE NOT NULL,
  PRIMARY KEY (file_id, line_no),
  CHECK (net = gross - fee - tax)
);

CREATE INDEX settlement_lines_bank_ref ON settlement_lines (bank_ref);

CREATE TABLE recon_results (
  id                BIGSERIAL PRIMARY KEY,
  file_id           UUID NOT NULL REFERENCES settlement_files (id), -- the reconciliation that produced it
  line_no           INT,                              -- NULL for missing_in_bank
  bank_ref          TEXT,
  item_kind         TEXT CHECK (item_kind IN ('payment', 'refund')),
  item_id           UUID,                             -- NULL when no ledger item was found
  status            TEXT NOT NULL CHECK (status IN
                      ('matched', 'fee_variance', 'amount_mismatch', 'duplicate', 'missing_in_ledger', 'missing_in_bank')),
  expected_gross    BIGINT,
  actual_gross      BIGINT,
  expected_fee      BIGINT,                           -- fee + tax per the bank's published schedule
  actual_fee        BIGINT,
  suspense          BIGINT NOT NULL DEFAULT 0,        -- signed debit this result put into recon_suspense
  resolution        TEXT CHECK (resolution IN ('write_off', 'bank_corrected', 'matched_late')),
  resolved_by_entry UUID REFERENCES journal_entries (id),
  resolved_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL,
  UNIQUE (file_id, line_no),
  CHECK ((resolution IS NULL) = (resolved_at IS NULL)),
  CHECK (status <> 'matched' OR resolution IS NULL)
);

-- A ledger item is settled at most once, and reported missing at most once.
CREATE UNIQUE INDEX recon_one_settlement_per_item ON recon_results (item_kind, item_id)
  WHERE status IN ('matched', 'fee_variance', 'amount_mismatch');
CREATE UNIQUE INDEX recon_one_missing_per_item ON recon_results (item_kind, item_id)
  WHERE status = 'missing_in_bank';
-- The exception queue.
CREATE INDEX recon_open_exceptions ON recon_results (created_at) WHERE status <> 'matched' AND resolved_at IS NULL;
