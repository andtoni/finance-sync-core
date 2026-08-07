-- Exact operator tombstones for erroneous provider observations. Raw evidence remains append-only;
-- this table is the separate projection authority for a bounded, audited quarantine.
CREATE TABLE IF NOT EXISTS observation_quarantines (
  quarantine_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  source_resource_incarnation_id uuid NOT NULL,
  observation_id uuid NOT NULL,
  provider_version text NOT NULL,
  semantic_hash char(64) NOT NULL,
  hash_specification_version integer NOT NULL CHECK (hash_specification_version > 0),
  reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 2048),
  actor_subject text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (
    deployment_id, local_owner_id, source_resource_incarnation_id,
    provider_version, semantic_hash, hash_specification_version
  ),
  FOREIGN KEY (deployment_id, local_owner_id, source_resource_incarnation_id)
    REFERENCES resource_incarnations(deployment_id, local_owner_id, source_resource_incarnation_id),
  FOREIGN KEY (deployment_id, local_owner_id, observation_id)
    REFERENCES raw_observations(deployment_id, local_owner_id, observation_id)
);
CREATE INDEX IF NOT EXISTS observation_quarantines_observation_idx
  ON observation_quarantines (deployment_id, local_owner_id, source_resource_incarnation_id, provider_version);
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT ON observation_quarantines TO finance_sync_runtime;
  REVOKE UPDATE, DELETE ON observation_quarantines FROM finance_sync_runtime;
 END IF;
END $$;
CREATE OR REPLACE FUNCTION reject_observation_quarantine_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'observation quarantines are append-only'; END $$;
DROP TRIGGER IF EXISTS observation_quarantines_append_only ON observation_quarantines;
CREATE TRIGGER observation_quarantines_append_only BEFORE UPDATE OR DELETE ON observation_quarantines FOR EACH ROW EXECUTE FUNCTION reject_observation_quarantine_mutation();

ALTER TABLE projection_outbox DROP CONSTRAINT IF EXISTS projection_outbox_state_check;
ALTER TABLE projection_outbox ADD CONSTRAINT projection_outbox_state_check
  CHECK (state IN ('pending', 'uncertain', 'delivered', 'dead_letter', 'suppressed'));
