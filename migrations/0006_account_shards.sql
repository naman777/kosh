-- Hot-account sharding (ADR 0003). The system accounts every payment
-- touches get N interchangeable shards; a payment always uses the shard
-- its id hashes to. Shard 0 is the only shard of every other account.
ALTER TABLE accounts ADD COLUMN shard INT NOT NULL DEFAULT 0 CHECK (shard >= 0);
ALTER TABLE accounts DROP CONSTRAINT accounts_code_owner_id_key;
ALTER TABLE accounts ADD CONSTRAINT accounts_code_owner_shard_key UNIQUE NULLS NOT DISTINCT (code, owner_id, shard);
