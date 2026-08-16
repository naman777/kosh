-- Double-entry ledger. Amounts are BIGINT paise. Rows are append-only:
-- postings are never updated or deleted, and a journal entry may only move
-- pending → posted or pending → voided. Triggers enforce both (invariant 11).

CREATE TABLE accounts (
  id          BIGSERIAL PRIMARY KEY,
  code        TEXT NOT NULL CHECK (code <> ''),       -- bank_receivable, merchant_payable, fee_revenue ...
  owner_id    UUID,                                   -- merchant id for merchant_payable
  normal_side TEXT NOT NULL CHECK (normal_side IN ('debit', 'credit')),
  posted_dr   BIGINT NOT NULL DEFAULT 0 CHECK (posted_dr  >= 0),
  posted_cr   BIGINT NOT NULL DEFAULT 0 CHECK (posted_cr  >= 0),
  pending_dr  BIGINT NOT NULL DEFAULT 0 CHECK (pending_dr >= 0),
  pending_cr  BIGINT NOT NULL DEFAULT 0 CHECK (pending_cr >= 0),
  flags       INT NOT NULL DEFAULT 0,                 -- 1 = debits_must_not_exceed_credits, 2 = credits_must_not_exceed_debits
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (code, owner_id)
);

CREATE TABLE journal_entries (
  id          UUID PRIMARY KEY,
  kind        TEXT NOT NULL CHECK (kind <> ''),       -- capture | refund | payout | adjustment | transfer ...
  ref_id      UUID NOT NULL,                          -- payment, refund or payout id
  state       TEXT NOT NULL CHECK (state IN ('posted', 'pending', 'voided')),
  expires_at  TIMESTAMPTZ,                            -- deadline for a pending hold
  created_at  TIMESTAMPTZ NOT NULL,
  resolved_at TIMESTAMPTZ,                            -- when a hold was posted or voided
  UNIQUE (kind, ref_id),                              -- one entry per business event
  -- A hold always has a deadline (invariant 9).
  CHECK (state <> 'pending' OR expires_at IS NOT NULL)
);

CREATE INDEX journal_entries_pending_expiry ON journal_entries (expires_at) WHERE state = 'pending';

CREATE TABLE postings (
  id         BIGSERIAL PRIMARY KEY,
  entry_id   UUID NOT NULL REFERENCES journal_entries (id),
  account_id BIGINT NOT NULL REFERENCES accounts (id),
  side       TEXT NOT NULL CHECK (side IN ('dr', 'cr')),
  amount     BIGINT NOT NULL CHECK (amount > 0)
);

CREATE INDEX postings_entry ON postings (entry_id);
CREATE INDEX postings_account ON postings (account_id);

-- Append-only guards ---------------------------------------------------------

CREATE FUNCTION ledger_reject_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger: % on % is not allowed; ledger rows are append-only', TG_OP, TG_TABLE_NAME
    USING ERRCODE = 'restrict_violation';
END
$$;

CREATE TRIGGER postings_append_only
  BEFORE UPDATE OR DELETE ON postings
  FOR EACH ROW EXECUTE FUNCTION ledger_reject_change();

CREATE TRIGGER postings_no_truncate
  BEFORE TRUNCATE ON postings
  FOR EACH STATEMENT EXECUTE FUNCTION ledger_reject_change();

CREATE TRIGGER journal_entries_no_truncate
  BEFORE TRUNCATE ON journal_entries
  FOR EACH STATEMENT EXECUTE FUNCTION ledger_reject_change();

-- Entries may only resolve a hold: pending → posted or pending → voided,
-- setting resolved_at and nothing else.
CREATE FUNCTION journal_entries_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ledger: journal entry % cannot be deleted', OLD.id
      USING ERRCODE = 'restrict_violation';
  END IF;
  IF OLD.state = 'pending'
     AND NEW.state IN ('posted', 'voided')
     AND NEW.id = OLD.id
     AND NEW.kind = OLD.kind
     AND NEW.ref_id = OLD.ref_id
     AND NEW.created_at = OLD.created_at
     AND NEW.expires_at IS NOT DISTINCT FROM OLD.expires_at
     AND NEW.resolved_at IS NOT NULL THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'ledger: journal entry % is immutable (% -> %)', OLD.id, OLD.state, NEW.state
    USING ERRCODE = 'restrict_violation';
END
$$;

CREATE TRIGGER journal_entries_guard
  BEFORE UPDATE OR DELETE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION journal_entries_guard();
