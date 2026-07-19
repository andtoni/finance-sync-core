-- Bind admissions and durable runtime work to the provider credential generation.
-- Existing rows predate this fence and are conservatively assigned the generation
-- current at migration time; a revoked row can therefore only be relinked after
-- its stored generation is explicitly attested.
ALTER TABLE provider_account_admissions
  ADD COLUMN IF NOT EXISTS provider_generation integer;
UPDATE provider_account_admissions paa
SET provider_generation = pc.provider_generation
FROM provider_connections pc
WHERE paa.provider_generation IS NULL
  AND pc.deployment_id = paa.deployment_id
  AND pc.local_owner_id = paa.local_owner_id
  AND pc.connection_id = paa.connection_id;
ALTER TABLE provider_account_admissions
  ALTER COLUMN provider_generation SET NOT NULL;
ALTER TABLE provider_account_admissions
  DROP CONSTRAINT IF EXISTS provider_account_admissions_provider_generation_check;
ALTER TABLE provider_account_admissions
  ADD CONSTRAINT provider_account_admissions_provider_generation_check
  CHECK (provider_generation > 0);

ALTER TABLE sync_checkpoints
  ADD COLUMN IF NOT EXISTS provider_generation integer NOT NULL DEFAULT 1;
ALTER TABLE sync_checkpoints
  DROP CONSTRAINT IF EXISTS sync_checkpoints_pkey;
ALTER TABLE sync_checkpoints
  ADD PRIMARY KEY (deployment_id, connection_id, provider_generation);
ALTER TABLE sync_checkpoints
  ADD CONSTRAINT sync_checkpoints_provider_generation_check
  CHECK (provider_generation > 0);

ALTER TABLE projection_outbox
  ADD COLUMN IF NOT EXISTS provider_generation integer NOT NULL DEFAULT 1;
ALTER TABLE projection_outbox
  DROP CONSTRAINT IF EXISTS projection_outbox_deployment_id_idempotency_key_key;
ALTER TABLE projection_outbox
  ADD CONSTRAINT projection_outbox_runtime_identity_key
  UNIQUE (deployment_id, connection_id, provider_generation, idempotency_key);
ALTER TABLE projection_outbox
  ADD CONSTRAINT projection_outbox_provider_generation_check
  CHECK (provider_generation > 0);
DROP INDEX IF EXISTS projection_outbox_unresolved_idx;
CREATE INDEX projection_outbox_unresolved_idx
  ON projection_outbox (deployment_id, connection_id, provider_generation, created_at)
  WHERE state IN ('pending', 'uncertain');

-- Generation rotation is the durable revocation point. Any unresolved bridge
-- work from the old runtime becomes terminal before the new runtime is visible.
CREATE OR REPLACE FUNCTION fence_provider_connection_generation() RETURNS trigger AS $$
BEGIN
  IF NEW.provider_generation < OLD.provider_generation THEN
    RAISE EXCEPTION 'provider generation cannot decrease';
  END IF;
  IF NEW.provider <> OLD.provider OR NEW.provider_connection_id <> OLD.provider_connection_id
     OR NEW.provider_principal_id <> OLD.provider_principal_id OR NEW.local_owner_id <> OLD.local_owner_id
     OR NEW.deployment_id <> OLD.deployment_id THEN
    RAISE EXCEPTION 'provider connection identity is immutable';
  END IF;
  IF NEW.secret_reference <> OLD.secret_reference AND NEW.provider_generation = OLD.provider_generation THEN
    RAISE EXCEPTION 'provider credential changes require a new provider generation';
  END IF;
  IF NEW.state = 'disabled' AND OLD.state <> 'disabled' AND NEW.provider_generation = OLD.provider_generation THEN
    RAISE EXCEPTION 'provider disable requires a new provider generation';
  END IF;
  IF NEW.provider_generation > OLD.provider_generation THEN
    UPDATE projection_outbox
    SET state = 'dead_letter', attempts = attempts + 1,
        last_error = 'PROVIDER_GENERATION_SUPERSEDED',
        dead_lettered_at = now(), dead_letter_reason = 'PROVIDER_GENERATION_SUPERSEDED'
    WHERE deployment_id = OLD.deployment_id
      AND split_part(connection_id, ':', 1) = OLD.connection_id::text
      AND provider_generation = OLD.provider_generation
      AND state IN ('pending', 'uncertain');
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
