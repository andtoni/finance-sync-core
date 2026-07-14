CREATE TABLE IF NOT EXISTS raw_observations (
 deployment_id text NOT NULL, local_owner_id text NOT NULL, observation_id uuid NOT NULL, source_resource_incarnation_id uuid NOT NULL,
 provider_version text, semantic_hash char(64) NOT NULL, hash_specification_version integer NOT NULL, payload jsonb NOT NULL,
 observed_at timestamptz NOT NULL, recorded_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (deployment_id, local_owner_id, observation_id),
 UNIQUE NULLS NOT DISTINCT (deployment_id, local_owner_id, source_resource_incarnation_id, provider_version, semantic_hash, hash_specification_version),
 FOREIGN KEY (deployment_id, local_owner_id, source_resource_incarnation_id) REFERENCES resource_incarnations(deployment_id, local_owner_id, source_resource_incarnation_id)
);
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT ON raw_observations TO finance_sync_runtime;
  REVOKE UPDATE, DELETE ON raw_observations FROM finance_sync_runtime;
 END IF;
END $$;
CREATE OR REPLACE FUNCTION reject_raw_observation_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'raw observations are append-only'; END $$;
DROP TRIGGER IF EXISTS raw_observations_append_only ON raw_observations;
CREATE TRIGGER raw_observations_append_only BEFORE UPDATE OR DELETE ON raw_observations FOR EACH ROW EXECUTE FUNCTION reject_raw_observation_mutation();
