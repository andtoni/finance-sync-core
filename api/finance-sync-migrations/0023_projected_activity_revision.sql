-- The receiver orders provider writes per written activity by observation
-- revision and rejects a different payload at an unchanged revision. Observation
-- ordinals identify raw provider facts, but the projected activity payload is
-- derived and can change without a new fact. This ledger records, per written
-- activity, each projected payload and the revision it was given, so a changed
-- payload always takes a strictly greater ordinal from the same sequence.
CREATE TABLE IF NOT EXISTS projected_activity_revision_ledger (
 deployment_id text NOT NULL,
 local_owner_id text NOT NULL,
 resource_id text NOT NULL,
 observation_revision bigint NOT NULL CHECK (observation_revision > 0),
 -- NULL only for a backfilled legacy revision that carried more than one
 -- payload. It never equals a projected payload, so the next projection of
 -- that activity takes a fresh ordinal.
 payload_digest char(64),
 recorded_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (deployment_id, local_owner_id, resource_id, observation_revision)
);
-- Seed from every revisioned activity upsert already enqueued or recovered,
-- whatever its delivery state: the receiver can hold at most the greatest
-- revision ever sent. A lifecycle migration writes its successor.
INSERT INTO projected_activity_revision_ledger (
 deployment_id, local_owner_id, resource_id, observation_revision, payload_digest
)
SELECT deployment_id, local_owner_id, resource_id, observation_revision,
       CASE WHEN count(DISTINCT activity) = 1
            THEN encode(sha256(convert_to(min(activity::text), 'UTF8')), 'hex') END
FROM (
  SELECT operation->>'deploymentId' AS deployment_id,
         operation->>'subject' AS local_owner_id,
         COALESCE(operation->>'lifecycleSuccessorId', operation->>'resourceId') AS resource_id,
         CASE WHEN operation->>'observationRevision' ~ '^[1-9][0-9]{0,15}$'
              THEN (operation->>'observationRevision')::bigint END AS observation_revision,
         operation->'activity' AS activity
  FROM (
    SELECT payload AS operation FROM projection_outbox
    UNION ALL
    SELECT ordinary_operation FROM investec_terminal_revision_recovery_intents
    WHERE ordinary_operation IS NOT NULL
  ) enqueued
  WHERE operation->>'kind' = 'ActivityUpsert'
    AND jsonb_typeof(operation->'activity') = 'object'
) projected
WHERE deployment_id IS NOT NULL AND local_owner_id IS NOT NULL
  AND resource_id IS NOT NULL AND observation_revision IS NOT NULL
GROUP BY deployment_id, local_owner_id, resource_id, observation_revision
ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION reject_projected_activity_revision_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'projected activity revisions are append-only'; END $$;
CREATE TRIGGER projected_activity_revision_ledger_append_only BEFORE UPDATE OR DELETE ON projected_activity_revision_ledger FOR EACH ROW EXECUTE FUNCTION reject_projected_activity_revision_mutation();
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT ON projected_activity_revision_ledger TO finance_sync_runtime;
  GRANT USAGE, SELECT ON SEQUENCE finance_sync_observation_revision_seq TO finance_sync_runtime;
  REVOKE UPDATE, DELETE ON projected_activity_revision_ledger FROM finance_sync_runtime;
 END IF;
END $$;
