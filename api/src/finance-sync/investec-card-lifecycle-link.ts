import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import { investecProviderFactPairShapeMatches } from './runtime-deps'
import type { ObservationOutput } from './probes/investec/investec-adapter'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

interface RecordValue { readonly [key: string]: unknown }
function record(value: unknown): Readonly<Record<string, unknown>> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined
}

export interface InvestecCardLifecycleLinkRequest {
  readonly sourceResourceIncarnationId: string
  readonly pendingTransactionId: string
  readonly postedTransactionId: string
  readonly reason: string
}

export interface InvestecCardLifecycleLinkResult {
  readonly overrideId: string
  readonly pendingResourceId: string
  readonly postedResourceId: string
  readonly inserted: boolean
  readonly delivered: number
}

export class InvestecCardLifecycleLinkError extends Error {
  constructor(readonly status: number, message: string) { super(message) }
}

interface SourceObservationRow {
  readonly payload: unknown
  readonly raw_payload: unknown
  readonly provider_version: string
  readonly semantic_hash: string
  readonly hash_specification_version: number
  readonly observed_at: string
}

interface LifecycleOverrideRow {
  readonly override_id: string
  readonly pending_transaction_id: string
  readonly posted_transaction_id: string
}

function validTransactionId(value: string): boolean {
  return value.length > 0 && value.length <= 512 && !/[\u0000-\u001f\u007f]/.test(value)
}

function transactionId(payload: Readonly<Record<string, unknown>>): string | undefined {
  const value = payload.transactionId ?? payload.transaction_id ?? payload.sourceId
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function providerTransactionType(payload: Readonly<Record<string, unknown>>): string | undefined {
  const direct = payload.transactionType ?? payload.sourceType
  return typeof direct === 'string' && direct.trim().toUpperCase() === 'CARDPURCHASES'
    ? 'CARDPURCHASES'
    : undefined
}

function connectionFor(
  connections: readonly RuntimeConnection[],
  providerAccountId: string,
  sourceResourceIncarnationId: string,
): RuntimeConnection {
  const matches = connections.filter(connection => connection.provider === 'investec'
    && connection.sourceResourceIncarnationId === sourceResourceIncarnationId
    && connection.accountIds.includes(providerAccountId))
  if (matches.length !== 1) {
    throw new InvestecCardLifecycleLinkError(409, 'Investec card lifecycle account authority is ambiguous')
  }
  return matches[0]
}

function resourceId(deploymentId: string, sourceResourceIncarnationId: string, transactionId: string): string {
  return `${deploymentId}:${sourceResourceIncarnationId}:${transactionId}`
}

function observationFromRow(
  deploymentId: string,
  localOwnerId: string,
  sourceResourceIncarnationId: string,
  row: SourceObservationRow,
): ObservationOutput {
  const payload = record(row.payload)
  if (!payload || !transactionId(payload) || !row.provider_version || !row.semantic_hash || !row.observed_at
    || !Number.isSafeInteger(row.hash_specification_version) || row.hash_specification_version < 1) {
    throw new InvestecCardLifecycleLinkError(422, 'selected card lifecycle observation is malformed')
  }
  return {
    deploymentId,
    localOwnerId,
    sourceResourceIncarnationId,
    providerVersion: row.provider_version,
    semanticHash: row.semantic_hash,
    hashSpecificationVersion: row.hash_specification_version,
    domain: 'cash_ledger',
    payload,
    ...(row.raw_payload === null || row.raw_payload === undefined ? {} : { rawPayload: row.raw_payload }),
    observedAt: row.observed_at,
  }
}

async function selectedObservations(
  sql: FinanceSyncSql,
  deploymentId: string,
  subject: string,
  request: InvestecCardLifecycleLinkRequest,
): Promise<{ readonly card: SourceObservationRow; readonly settled: SourceObservationRow }> {
  const cardProviderVersion = `investec:card:${request.pendingTransactionId}`
  const settledProviderVersion = `investec:txn:${request.postedTransactionId}`
  const rows = await sql<SourceObservationRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (provider_version)
        payload, raw_payload, provider_version, semantic_hash,
        hash_specification_version, observed_at, deployment_id,
        local_owner_id, source_resource_incarnation_id
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND local_owner_id = ${subject}
        AND source_resource_incarnation_id = ${request.sourceResourceIncarnationId}::uuid
        AND (provider_version = ${cardProviderVersion} OR provider_version = ${settledProviderVersion})
      ORDER BY provider_version, recorded_at DESC, observed_at DESC, observation_id DESC
    )
    SELECT payload, raw_payload, provider_version, semantic_hash,
      hash_specification_version, observed_at
    FROM latest
    WHERE payload->>'isRemoved' IS DISTINCT FROM 'true'
      AND NOT EXISTS (
        SELECT 1 FROM observation_quarantines q
        WHERE q.deployment_id = latest.deployment_id AND q.local_owner_id = latest.local_owner_id
          AND q.source_resource_incarnation_id = latest.source_resource_incarnation_id
          AND q.provider_version = latest.provider_version AND q.semantic_hash = latest.semantic_hash
          AND q.hash_specification_version = latest.hash_specification_version
      )
  `
  const card = rows.find(row => row.provider_version === cardProviderVersion
    && record(row.payload)?.transactionId === request.pendingTransactionId)
  const settled = rows.find(row => row.provider_version === settledProviderVersion
    && record(row.payload)?.transactionId === request.postedTransactionId)
  if (!card || !settled) {
    throw new InvestecCardLifecycleLinkError(404, 'selected Investec card lifecycle observations were not found')
  }
  return { card, settled }
}

async function existingOverride(
  sql: FinanceSyncSql,
  deploymentId: string,
  subject: string,
  request: InvestecCardLifecycleLinkRequest,
): Promise<readonly LifecycleOverrideRow[]> {
  return sql<LifecycleOverrideRow[]>`
    SELECT override_id, pending_transaction_id, posted_transaction_id
    FROM investec_card_lifecycle_overrides
    WHERE deployment_id = ${deploymentId}
      AND local_owner_id = ${subject}
      AND source_resource_incarnation_id = ${request.sourceResourceIncarnationId}::uuid
      AND (pending_transaction_id IN (${request.pendingTransactionId}, ${request.postedTransactionId})
        OR posted_transaction_id IN (${request.pendingTransactionId}, ${request.postedTransactionId}))
  `
}

/**
 * User-authorized Investec card lifecycle override. Raw observations remain
 * immutable; the durable override only relaxes the amount equality fence for
 * this exact pending/posted provider pair and reuses normal projection.
 */
export function createInvestecCardLifecycleLinkControl(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
) {
  return {
    async create(rawSubject: string, request: InvestecCardLifecycleLinkRequest): Promise<InvestecCardLifecycleLinkResult> {
      let subject: string
      try { subject = assertIssuerQualifiedSubject(rawSubject) }
      catch { throw new InvestecCardLifecycleLinkError(400, 'invalid subject') }
      if (!validTransactionId(request.pendingTransactionId)
        || !validTransactionId(request.postedTransactionId)
        || request.pendingTransactionId === request.postedTransactionId
        || typeof request.reason !== 'string'
        || request.reason.trim().length < 1
        || request.reason.trim().length > 2048
        || /[\u0000-\u001f\u007f]/.test(request.reason)) {
        throw new InvestecCardLifecycleLinkError(400, 'invalid Investec card lifecycle link request')
      }

      const { card, settled } = await selectedObservations(sql, deploymentId, subject, request)
      const cardPayload = record(card.payload)
      const settledPayload = record(settled.payload)
      if (!cardPayload || !settledPayload
        || cardPayload.accountId !== settledPayload.accountId
        || typeof cardPayload.accountId !== 'string'
        || cardPayload.status?.toString().toUpperCase() !== 'PENDING'
        || settledPayload.status?.toString().toUpperCase() !== 'POSTED'
        || providerTransactionType(settledPayload) !== 'CARDPURCHASES'
        || !investecProviderFactPairShapeMatches(cardPayload, settledPayload)) {
        throw new InvestecCardLifecycleLinkError(409, 'selected Investec card lifecycle facts are incompatible')
      }

      const connections = await deps.loadConnections(deploymentId)
      const connection = connectionFor(connections, cardPayload.accountId, request.sourceResourceIncarnationId)
      const prior = await existingOverride(sql, deploymentId, subject, request)
      const samePrior = prior.find(row => row.pending_transaction_id === request.pendingTransactionId
        && row.posted_transaction_id === request.postedTransactionId)
      if (prior.length > 0 && !samePrior) {
        throw new InvestecCardLifecycleLinkError(409, 'one of the selected Investec card facts is already linked')
      }

      let overrideId = samePrior?.override_id
      let inserted = false
      if (!overrideId) {
        const insertedRows = await sql<{ override_id: string }[]>`
          INSERT INTO investec_card_lifecycle_overrides (
            deployment_id, local_owner_id, source_resource_incarnation_id,
            pending_transaction_id, posted_transaction_id, actor_subject, reason
          ) VALUES (
            ${deploymentId}, ${subject}, ${request.sourceResourceIncarnationId}::uuid,
            ${request.pendingTransactionId}, ${request.postedTransactionId}, ${subject}, ${request.reason.trim()}
          )
          ON CONFLICT DO NOTHING
          RETURNING override_id
        `
        overrideId = insertedRows[0]?.override_id
        inserted = overrideId !== undefined
        if (!overrideId) {
          const raced = await existingOverride(sql, deploymentId, subject, request)
          const exact = raced.find(row => row.pending_transaction_id === request.pendingTransactionId
            && row.posted_transaction_id === request.postedTransactionId)
          if (!exact) throw new InvestecCardLifecycleLinkError(409, 'one of the selected Investec card facts is already linked')
          overrideId = exact.override_id
        }
      }

      // Re-submit the posted fact through the normal Investec projection path.
      // The matcher now sees the durable override and emits the existing atomic
      // pending -> posted bridge migration.
      const postedObservation = observationFromRow(
        deploymentId, subject, request.sourceResourceIncarnationId, settled,
      )
      const pendingObservation = observationFromRow(
        deploymentId, subject, request.sourceResourceIncarnationId, card,
      )
      // Keep the pending observation visible to the projection transaction so a
      // retry after a prior local migration can reconstruct the source row.
      await deps.persistAndEnqueue(connection, [pendingObservation, postedObservation], { skipCurrentState: true })
      const projection = await deps.projectPending(connection)
      if (projection.failed > 0) {
        throw new InvestecCardLifecycleLinkError(503, 'Investec card lifecycle link projection is unresolved')
      }

      return {
        overrideId,
        pendingResourceId: resourceId(deploymentId, request.sourceResourceIncarnationId, request.pendingTransactionId),
        postedResourceId: resourceId(deploymentId, request.sourceResourceIncarnationId, request.postedTransactionId),
        inserted,
        delivered: projection.delivered,
      }
    },
  }
}
