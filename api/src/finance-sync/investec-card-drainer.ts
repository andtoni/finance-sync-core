import { startDrainer, type DrainerConfig, type DrainerHandle, type PulledMessage } from '../drain/drainer'
import type { ObservationOutput } from './probes/investec/investec-adapter'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'
import { readExclusiveInput } from './secret-input'
import { logFailure } from './failure-reason'
import { parseInvestecCardDelivery, sanitizeInvestecCardEnvelope } from './investec-card-code-v1'
import { investecCardObservation, accountMask, type InvestecCardPayload } from './investec-card-observation'
export { investecCardObservation, accountMask, type InvestecCardPayload } from './investec-card-observation'

interface RecordValue { readonly [key: string]: unknown }
function record(value: unknown): RecordValue | undefined { return value && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined }
function stringValue(value: unknown): string | undefined { return typeof value === 'string' && value.trim() ? value.trim() : undefined }
interface QueueEnvelope extends RecordValue { readonly source?: unknown; readonly payload?: unknown; readonly received_at?: unknown }
interface CardAccountMapping {
  readonly local_owner_id: string; readonly connection_id: string; readonly provider_account_id: string;
  readonly wealthfolio_account_id: string; readonly source_resource_incarnation_id: string; readonly currency: string; readonly mask: string
}
const CARD_HASH_SPECIFICATION_VERSION = 5
async function exactCardAccount(sql: FinanceSyncSql, deploymentId: string, mask: string): Promise<CardAccountMapping> {
  const rows = await sql<CardAccountMapping[]>`
    SELECT dpa.local_owner_id, dpa.connection_id::text, dpa.provider_account_id,
      paa.wealthfolio_account_id, dpa.source_resource_incarnation_id::text,
      dpa.currency, dpa.mask
    FROM discovered_provider_accounts dpa
    JOIN provider_connections pc
      ON pc.deployment_id = dpa.deployment_id AND pc.local_owner_id = dpa.local_owner_id
      AND pc.connection_id = dpa.connection_id
    JOIN provider_account_admissions paa
      ON paa.deployment_id = dpa.deployment_id AND paa.local_owner_id = dpa.local_owner_id
      AND paa.connection_id = dpa.connection_id AND paa.provider_account_id = dpa.provider_account_id
    WHERE dpa.deployment_id = ${deploymentId}
      AND dpa.mask = ${mask} AND pc.provider = 'investec' AND pc.state = 'active'
      AND pc.disabled_at IS NULL AND paa.revoked_at IS NULL AND paa.baseline_state = 'verified'
  `
  if (rows.length !== 1) throw new Error('Investec card account admission is absent or ambiguous')
  return rows[0]
}

interface RetainedCardObservationRow {
  readonly payload: unknown
  readonly raw_payload: unknown
  readonly provider_version: string
  readonly semantic_hash: string
  readonly hash_specification_version: number
  readonly observed_at: string | Date
  readonly source_resource_incarnation_id: string
}

/** True when the stored observation is the retirement of an authorization the provider never settled. */
function isRetiredAuthorization(payload: RecordValue | undefined): boolean {
  return record(record(record(payload?.metadata)?.providerOwned)?.investec)?.authorizationRetired === true
}

async function exactRetainedCardAccount(
  sql: FinanceSyncSql,
  deploymentId: string,
  providerAccountId: string,
  sourceResourceIncarnationId: string,
): Promise<CardAccountMapping> {
  const rows = await sql<CardAccountMapping[]>`
    SELECT dpa.local_owner_id, dpa.connection_id::text, dpa.provider_account_id,
      paa.wealthfolio_account_id, dpa.source_resource_incarnation_id::text,
      dpa.currency, dpa.mask
    FROM discovered_provider_accounts dpa
    JOIN provider_connections pc
      ON pc.deployment_id = dpa.deployment_id AND pc.local_owner_id = dpa.local_owner_id
      AND pc.connection_id = dpa.connection_id
    JOIN provider_account_admissions paa
      ON paa.deployment_id = dpa.deployment_id AND paa.local_owner_id = dpa.local_owner_id
      AND paa.connection_id = dpa.connection_id AND paa.provider_account_id = dpa.provider_account_id
    WHERE dpa.deployment_id = ${deploymentId}
      AND dpa.provider_account_id = ${providerAccountId}
      AND dpa.source_resource_incarnation_id = ${sourceResourceIncarnationId}::uuid
      AND pc.provider = 'investec' AND pc.state = 'active'
      AND pc.disabled_at IS NULL AND paa.revoked_at IS NULL AND paa.baseline_state = 'verified'
  `
  if (rows.length !== 1) throw new Error('Investec retained card account admission is absent or ambiguous')
  return rows[0]
}

export async function reprojectRetainedInvestecCards(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
): Promise<number> {
  const rows = await sql<RetainedCardObservationRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (o.source_resource_incarnation_id, o.payload->>'transactionId')
        payload, raw_payload, provider_version, semantic_hash, hash_specification_version, observed_at, source_resource_incarnation_id::text
      FROM raw_observations o
      WHERE o.deployment_id = ${deploymentId}
        AND o.provider_version LIKE 'investec:card:%'
        AND o.raw_payload IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM observation_quarantines q
          WHERE q.deployment_id = o.deployment_id
            AND q.local_owner_id = o.local_owner_id
            AND q.source_resource_incarnation_id = o.source_resource_incarnation_id
            AND q.provider_version = o.provider_version
            AND q.semantic_hash = o.semantic_hash
            AND q.hash_specification_version = o.hash_specification_version
        )
      ORDER BY o.source_resource_incarnation_id, o.payload->>'transactionId', o.recorded_at DESC, o.observation_id DESC
    )
    SELECT payload, raw_payload, provider_version, semantic_hash, hash_specification_version, observed_at, source_resource_incarnation_id
    FROM latest
  `
  if (rows.length === 0) return 0
  const connections = await deps.loadConnections(deploymentId)
  const batches = new Map<string, { connection: RuntimeConnection; observations: ObservationOutput[] }>()
  const seen = new Set<string>()
  let unresolved = false
  for (const row of rows) {
    const normalizedPayload = record(row.payload)
    const providerAccountId = stringValue(normalizedPayload?.accountId)
    const rawEvidence = record(row.raw_payload)
    const versioned = rawEvidence && (
      parseInvestecCardDelivery(rawEvidence)
      ?? parseInvestecCardDelivery(record(rawEvidence.payload))
    )
    const rawPayload = versioned?.transaction ?? (rawEvidence?.schemaVersion === undefined ? rawEvidence : undefined)
    // Legacy retained card observations without an exact provider date are
    // intentionally skipped; replay must not fabricate an authorization time.
    if (!providerAccountId || !rawPayload) continue
    let mapping: CardAccountMapping
    try {
      mapping = await exactRetainedCardAccount(sql, deploymentId, providerAccountId, row.source_resource_incarnation_id)
    } catch {
      unresolved = true
      continue
    }
    const connection = connections.find(candidate => candidate.provider === 'investec'
      && candidate.connectionId === `${mapping.connection_id}:${mapping.provider_account_id}`
      && candidate.sourceResourceIncarnationId === row.source_resource_incarnation_id)
    if (!connection) {
      unresolved = true
      continue
    }
    const observedAt = row.observed_at instanceof Date ? row.observed_at.toISOString() : row.observed_at
    if (normalizedPayload && isRetiredAuthorization(normalizedPayload)) {
      // The latest fact for this hold is its retirement. Rebuilding the hold
      // from the retained card payload would project it as pending again, so
      // project the stored retirement itself.
      const identity = `${connection.connectionId}:${row.semantic_hash}`
      if (seen.has(identity)) continue
      seen.add(identity)
      const batch = batches.get(connection.connectionId) ?? { connection, observations: [] }
      batch.observations.push({
        deploymentId,
        localOwnerId: connection.localOwnerId,
        sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
        providerVersion: row.provider_version,
        semanticHash: row.semantic_hash,
        hashSpecificationVersion: row.hash_specification_version,
        domain: 'cash_ledger',
        payload: normalizedPayload,
        rawPayload: row.raw_payload,
        observedAt,
      })
      batches.set(connection.connectionId, batch)
      continue
    }
    const observation = investecCardObservation({
      deploymentId,
      localOwnerId: connection.localOwnerId,
      sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
      providerAccountId: mapping.provider_account_id,
      accountCurrency: mapping.currency,
      payload: rawPayload as InvestecCardPayload,
      receivedAt: row.observed_at instanceof Date ? row.observed_at.toISOString() : row.observed_at,
    })
    if (!observation) {
      unresolved = true
      continue
    }
    if (observation.semanticHash === row.semantic_hash
      && row.hash_specification_version === CARD_HASH_SPECIFICATION_VERSION) continue
    const identity = `${connection.connectionId}:${observation.semanticHash}`
    if (seen.has(identity)) continue
    seen.add(identity)
    const batch = batches.get(connection.connectionId) ?? { connection, observations: [] }
    batch.observations.push(observation)
    batches.set(connection.connectionId, batch)
  }
  if (unresolved) throw new Error('Investec retained card projection is unresolved')
  let reprojected = 0
  let failed = false
  for (const { connection, observations } of batches.values()) {
    try {
      await deps.persistAndEnqueue(connection, observations, { skipCurrentState: true })
      const projection = await deps.projectPending(connection)
      if (projection.failed > 0) {
        failed = true
        continue
      }
      reprojected += observations.length
    } catch {
      failed = true
    }
  }
  if (failed) throw new Error('Investec retained card projection is unresolved')
  return reprojected
}

interface CardIngressQuarantineRow {
  readonly quarantine_id: string
  readonly envelope: unknown
}

function decodeStoredCardEnvelope(value: unknown): RecordValue | undefined {
  const decoded = record(value)
  if (decoded) return decoded
  if (typeof value !== 'string') return undefined
  try {
    return record(JSON.parse(value))
  } catch {
    return undefined
  }
}

function nonZeroCardCents(value: unknown): boolean {
  if (typeof value === 'number') return Number.isSafeInteger(value) && value > 0
  return typeof value === 'string' && /^\d+$/.test(value.trim()) && !/^0+$/.test(value.trim())
}

/** Replay retained direct card deliveries without mutating the quarantine evidence. */
export async function replayQuarantinedInvestecCards(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
): Promise<number> {
  const rows = await sql<CardIngressQuarantineRow[]>`
    SELECT quarantine_id, envelope
    FROM card_ingress_quarantines
    WHERE deployment_id = ${deploymentId}
      AND source = 'investec_card_code'
      AND reason = 'INVALID_CARD_CODE_V1'
    ORDER BY created_at, quarantine_id
  `
  if (rows.length === 0) return 0
  const connections = await deps.loadConnections(deploymentId)
  let replayed = 0
  for (const row of rows) {
    const envelope = decodeStoredCardEnvelope(row.envelope)
    const payload = record(envelope?.payload)
    const versioned = payload ? parseInvestecCardDelivery(payload) : undefined
    if (!versioned || versioned.eventType !== 'after_transaction' || !nonZeroCardCents(versioned.transaction.centsAmount)) continue
    await handleInvestecCardMessage(
      sql,
      deploymentId,
      deps,
      { lease_id: `quarantine:${row.quarantine_id}`, id: row.quarantine_id, attempts: 0, envelope },
      connections,
    )
    replayed++
  }
  return replayed
}

async function quarantineCardIngress(sql: FinanceSyncSql, deploymentId: string, reason: string, envelope: unknown): Promise<void> {
  await sql`
    INSERT INTO card_ingress_quarantines (deployment_id, source, reason, envelope)
    VALUES (${deploymentId}, 'investec_card_code', ${reason}, ${sql.json(sanitizeInvestecCardEnvelope(envelope) as never)})
  `
}

async function handleInvestecCardMessage(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
  message: PulledMessage,
  connections: readonly RuntimeConnection[],
): Promise<boolean> {
  const envelope = record(message.envelope) as QueueEnvelope | undefined
  if (envelope?.source !== 'investec') return false
  const rawPayload = record(envelope.payload)
  if (!rawPayload) { await quarantineCardIngress(sql, deploymentId, 'MISSING_PAYLOAD', envelope); return true }
  const versioned = parseInvestecCardDelivery(rawPayload)
  if (!versioned) { await quarantineCardIngress(sql, deploymentId, 'INVALID_CARD_CODE_V1', envelope); return true }
  if (versioned.eventType === 'after_decline') { await quarantineCardIngress(sql, deploymentId, 'AFTER_DECLINE', envelope); return true }
  const payload = versioned.transaction as InvestecCardPayload
  const mask = accountMask(payload.accountNumber)
  if (!mask) { await quarantineCardIngress(sql, deploymentId, 'MISSING_ACCOUNT_NUMBER', envelope); return true }
  let observationMapping: CardAccountMapping
  try {
    observationMapping = await exactCardAccount(sql, deploymentId, mask)
  } catch {
    await quarantineCardIngress(sql, deploymentId, 'AMBIGUOUS_OR_UNADMITTED_ACCOUNT', envelope)
    return true
  }
  const connectionId = `${observationMapping.connection_id}:${observationMapping.provider_account_id}`
  const connection = connections.find(candidate => candidate.provider === 'investec' && candidate.connectionId === connectionId)
  if (!connection) {
    await quarantineCardIngress(sql, deploymentId, 'INACTIVE_CONNECTION', envelope)
    return true
  }
  const observation = investecCardObservation({
    deploymentId,
    localOwnerId: connection.localOwnerId,
    sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
    providerAccountId: observationMapping.provider_account_id,
    accountCurrency: observationMapping.currency,
    payload,
    receivedAt: versioned?.providerEventTime ?? stringValue(envelope.received_at),
  })
  if (!observation) { await quarantineCardIngress(sql, deploymentId, 'INVALID_CARD_TRANSACTION', envelope); return true }
  await deps.persistAndEnqueue(connection, [{ ...observation, rawPayload: sanitizeInvestecCardEnvelope(envelope) }], { skipCurrentState: true })
  const projection = await deps.projectPending(connection)
  if (projection.failed > 0) throw new Error('Investec card projection is unresolved')
  return true
}

function drainerConfig(env: Readonly<Record<string, string | undefined>>): DrainerConfig | undefined {
  const names = ['CLOUDFLARE_ACCOUNT_ID', 'CF_QUEUE_ID', 'CF_QUEUE_PULL_TOKEN'] as const
  const values = names.map(name => readExclusiveInput(env, name))
  if (values.every(value => value === undefined)) return undefined
  if (values.some(value => value === undefined)) throw new Error('Investec card drainer configuration is incomplete')
  return {
    accountId: values[0]!, queueId: values[1]!, pullToken: values[2]!,
    batchSize: 50, visibilityTimeoutMs: 30_000, idlePollMs: 12_000,
  }
}

export function startInvestecCardDrainer(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
  env: Readonly<Record<string, string | undefined>> = process.env,
): DrainerHandle | undefined {
  const config = drainerConfig(env)
  if (!config) return undefined
  return startDrainer(config, {
    fetchImpl: fetch,
    // Admission and generation authority can change while the process lives.
    // Reload for every message so a valid event is never acknowledged against stale authority.
    handle: async message => handleInvestecCardMessage(
      sql,
      deploymentId,
      deps,
      message,
      await deps.loadConnections(deploymentId),
    ),
    log: (level, _message, error) => {
      if (level === 'error') logFailure('FINANCE_SYNC_WEBHOOK_DRAIN_RETRY', error, { stage: 'handler' })
      else if (level === 'warn') logFailure('FINANCE_SYNC_WEBHOOK_DRAIN_RETRY', error, { stage: 'pass' })
    },
  })
}
