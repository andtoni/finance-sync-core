import { withProjectedRevision, type BridgeDisclosureOperation } from './bridge-client'
import { observationRevisionNumber, type FinanceSyncSql } from '../storage/pg'

/**
 * Assign the revision an activity upsert carries to the receiver.
 *
 * The receiver orders provider writes per written activity by revision and
 * rejects a different payload at an unchanged revision. Observation ordinals
 * identify raw provider facts, while the projected payload is derived and can
 * change without a new fact (card/settlement correlation, review markers,
 * projection changes between releases). The ledger therefore records the last
 * payload projected for each written activity: the same payload keeps its
 * revision, and a different payload always takes a strictly greater one from
 * the shared observation ordinal sequence.
 *
 * Must run inside the READ COMMITTED transaction that makes the operation
 * durable; the transaction-scoped lock serializes concurrent projections of
 * one activity so each sees the previously committed payload.
 */
export async function assignProjectedRevision(
  tx: FinanceSyncSql,
  operation: BridgeDisclosureOperation,
): Promise<BridgeDisclosureOperation> {
  if (operation.kind !== 'ActivityUpsert' || operation.activity === undefined) return operation
  // A lifecycle migration writes its successor, never its pending predecessor.
  const resourceId = operation.lifecycleSuccessorId ?? operation.resourceId
  const lockKey = ['finance-sync-projected-revision', operation.deploymentId, operation.subject, resourceId].join('\x1f')
  await tx`SELECT pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`
  const [latest] = await tx<{ observation_revision: string | number; same_payload: boolean | null }[]>`
    SELECT observation_revision,
      payload_digest = encode(sha256(convert_to((${tx.json(operation.activity as never)}::jsonb)::text, 'UTF8')), 'hex') AS same_payload
    FROM projected_activity_revision_ledger
    WHERE deployment_id = ${operation.deploymentId}
      AND local_owner_id = ${operation.subject}
      AND resource_id = ${resourceId}
    ORDER BY observation_revision DESC
    LIMIT 1
  `
  const latestRevision = latest ? observationRevisionNumber(latest.observation_revision) : undefined
  if (latestRevision !== undefined && latest.same_payload === true) return withProjectedRevision(operation, latestRevision)
  // An observation ordinal newer than everything projected for this activity
  // is already strictly greater; anything else needs a fresh ordinal.
  const incoming = operation.observationRevision
  const retained = incoming !== undefined && (latestRevision === undefined || incoming > latestRevision) ? incoming : null
  const [recorded] = await tx<{ observation_revision: string | number }[]>`
    INSERT INTO projected_activity_revision_ledger (
      deployment_id, local_owner_id, resource_id, observation_revision, payload_digest
    ) VALUES (
      ${operation.deploymentId}, ${operation.subject}, ${resourceId},
      COALESCE(${retained}::bigint, nextval('finance_sync_observation_revision_seq')),
      encode(sha256(convert_to((${tx.json(operation.activity as never)}::jsonb)::text, 'UTF8')), 'hex')
    )
    RETURNING observation_revision
  `
  const revision = retained ?? observationRevisionNumber(recorded.observation_revision)
  if (latestRevision !== undefined && revision <= latestRevision) throw new Error('projected activity revision is not strictly greater')
  return withProjectedRevision(operation, revision)
}
