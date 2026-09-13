-- Bound delivered/uncertain Investec lifecycle lookups to their exact durable identity.
-- Resource and successor IDs carry the source incarnation, while the partial
-- predicate excludes unrelated outbox history and terminal non-lifecycle work.
CREATE INDEX IF NOT EXISTS projection_outbox_investec_lifecycle_identity_idx
  ON projection_outbox (
    deployment_id,
    local_owner_id,
    connection_id,
    (payload->>'accountId'),
    (payload->>'subject'),
    (payload->>'resourceId'),
    (payload->>'lifecycleSuccessorId')
  )
  WHERE state IN ('delivered', 'uncertain')
    AND payload->>'kind' = 'ActivityUpsert'
    AND payload->>'lifecycleSuccessorId' IS NOT NULL;
