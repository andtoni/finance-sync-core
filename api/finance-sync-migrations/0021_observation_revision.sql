CREATE SEQUENCE IF NOT EXISTS finance_sync_observation_revision_seq;
-- Legacy raw observations may have NULL provider_version. They remain intact and
-- deliberately receive no revision until a provider-owned identity exists; the
-- ledger itself never invents a provider identity for retained evidence.
CREATE TABLE IF NOT EXISTS observation_revision_ledger (
 deployment_id text NOT NULL,
 local_owner_id text NOT NULL,
 source_resource_incarnation_id uuid NOT NULL,
 provider_version text NOT NULL,
 semantic_hash char(64) NOT NULL,
 hash_specification_version integer NOT NULL,
 observation_revision bigint NOT NULL UNIQUE,
 PRIMARY KEY (deployment_id, local_owner_id, source_resource_incarnation_id, provider_version, semantic_hash, hash_specification_version)
);
INSERT INTO observation_revision_ledger (
 deployment_id, local_owner_id, source_resource_incarnation_id, provider_version,
 semantic_hash, hash_specification_version, observation_revision
)
SELECT deployment_id, local_owner_id, source_resource_incarnation_id, provider_version,
       semantic_hash, hash_specification_version,
       row_number() OVER (ORDER BY recorded_at, observation_id)::bigint
FROM raw_observations
WHERE provider_version IS NOT NULL
ON CONFLICT DO NOTHING;
SELECT setval(
  'finance_sync_observation_revision_seq',
  COALESCE((SELECT max(observation_revision) FROM observation_revision_ledger), 1),
  (SELECT count(*) > 0 FROM observation_revision_ledger)
);
CREATE OR REPLACE FUNCTION reject_observation_revision_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'observation revisions are append-only'; END $$;
CREATE TRIGGER observation_revision_ledger_append_only BEFORE UPDATE OR DELETE ON observation_revision_ledger FOR EACH ROW EXECUTE FUNCTION reject_observation_revision_mutation();
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT ON observation_revision_ledger TO finance_sync_runtime;
  GRANT USAGE, SELECT ON SEQUENCE finance_sync_observation_revision_seq TO finance_sync_runtime;
  REVOKE UPDATE, DELETE ON observation_revision_ledger FROM finance_sync_runtime;
 END IF;
END $$;
