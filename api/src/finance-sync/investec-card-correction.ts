import { createHash } from 'node:crypto'
import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import { parseInvestecCardDelivery, sanitizeInvestecCardEnvelope } from './investec-card-code-v1'
import { investecCardObservation, type InvestecCardPayload } from './investec-card-observation'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

interface RecordValue { readonly [key: string]: unknown }
function record(value: unknown): RecordValue | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined
}
function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

export interface InvestecCardCurrencyCorrectionRequest {
  readonly sourceResourceIncarnationId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
  readonly amount: string
  readonly currency: string
  readonly reason: string
}

export interface InvestecCardCurrencyCorrectionResult {
  readonly correctionId: string
  readonly observationId: string
  readonly quarantineId: string
  readonly resourceId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly inserted: boolean
  readonly delivered: number
}

export class InvestecCardCurrencyCorrectionError extends Error {
  constructor(readonly status: number, message: string) { super(message) }
}

interface SourceObservationRow {
  readonly observation_id: string
  readonly quarantine_id: string
  readonly payload: unknown
  readonly raw_payload: unknown
  readonly observed_at: string
  readonly provider_account_id: string | null
  readonly account_currency: string | null
}

function positiveDecimalToCents(value: string): string | undefined {
  const trimmed = value.trim()
  const match = /^(\d+)(?:\.(\d{1,2}))?$/.exec(trimmed)
  if (!match) return undefined
  const fraction = (match[2] ?? '').padEnd(2, '0')
  const cents = BigInt(`${match[1]}${fraction}`)
  return cents > 0n ? cents.toString() : undefined
}

function providerPayload(rawPayload: unknown, normalized: RecordValue): InvestecCardPayload {
  const raw = record(rawPayload)
  const versioned = raw ? parseInvestecCardDelivery(raw) : undefined
  const transaction = versioned?.transaction ?? record(raw?.transaction) ?? raw
  if (transaction) return transaction as InvestecCardPayload
  const transactionDate = stringValue(normalized.authorizationDateTime)
    ?? (stringValue(normalized.transactionDate) ? `${normalized.transactionDate}T00:00:00.000Z` : undefined)
  const amount = stringValue(normalized.amount)?.replace('-', '')
  if (!transactionDate || !amount) throw new InvestecCardCurrencyCorrectionError(422, 'retained card payload lacks provider time or amount evidence')
  const centsAmount = positiveDecimalToCents(amount)
  if (!centsAmount) throw new InvestecCardCurrencyCorrectionError(422, 'retained card payload amount cannot be represented in cents')
  return {
    centsAmount,
    dateTime: transactionDate,
    type: normalized.type,
    reference: normalized.reference,
    merchant: { name: normalized.description },
    card: normalized.cardNumber ? { display: normalized.cardNumber } : undefined,
  }
}

function correctionId(request: InvestecCardCurrencyCorrectionRequest): string {
  return createHash('sha256').update([
    'investec-card-owner-currency-correction-v1', request.sourceResourceIncarnationId,
    request.providerVersion, request.semanticHash, String(request.hashSpecificationVersion),
    request.amount, request.currency,
  ].join('\x00'), 'utf8').digest('hex')
}

function connectionFor(
  connections: readonly RuntimeConnection[],
  providerAccountId: string,
  sourceResourceIncarnationId: string,
): RuntimeConnection {
  const matches = connections.filter(connection => connection.provider === 'investec'
    && connection.sourceResourceIncarnationId === sourceResourceIncarnationId
    && connection.accountIds.includes(providerAccountId))
  if (matches.length !== 1) throw new InvestecCardCurrencyCorrectionError(409, 'Investec card correction account authority is ambiguous')
  return matches[0]
}

/**
 * Owner-authorized correction for a quarantined card currency/original amount.
 *
 * The original raw observation and quarantine are immutable. This appends a new
 * normalized observation with the same provider transaction identity, an audit
 * envelope pointing at the original evidence, and projects it through the
 * ordinary outbox/bridge path. It is intentionally not a database release or
 * direct Wealthfolio mutation.
 */
export function createInvestecCardCurrencyCorrectionControl(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
) {
  return {
    async create(rawSubject: string, request: InvestecCardCurrencyCorrectionRequest): Promise<InvestecCardCurrencyCorrectionResult> {
      let subject: string
      try { subject = assertIssuerQualifiedSubject(rawSubject) }
      catch { throw new InvestecCardCurrencyCorrectionError(400, 'invalid subject') }
      if (request.currency !== request.currency.toUpperCase() || !/^[A-Z]{3}$/.test(request.currency)) {
        throw new InvestecCardCurrencyCorrectionError(400, 'currency must be an uppercase ISO code')
      }
      if (!request.providerVersion.startsWith('investec:card:')) {
        throw new InvestecCardCurrencyCorrectionError(400, 'only Investec card observations may be corrected')
      }
      const centsAmount = positiveDecimalToCents(request.amount)
      if (!centsAmount) throw new InvestecCardCurrencyCorrectionError(400, 'amount must be a positive decimal with at most two fractional digits')
      const rows = await sql<SourceObservationRow[]>`
        SELECT o.observation_id::text, q.quarantine_id::text, o.payload, o.raw_payload,
          o.observed_at::text, o.payload->>'accountId' AS provider_account_id,
          dpa.currency AS account_currency
        FROM raw_observations o
        JOIN observation_quarantines q
          ON q.deployment_id = o.deployment_id
          AND q.local_owner_id = o.local_owner_id
          AND q.source_resource_incarnation_id = o.source_resource_incarnation_id
          AND q.provider_version = o.provider_version
          AND q.semantic_hash = o.semantic_hash
          AND q.hash_specification_version = o.hash_specification_version
        LEFT JOIN discovered_provider_accounts dpa
          ON dpa.deployment_id = o.deployment_id
          AND dpa.local_owner_id = o.local_owner_id
          AND dpa.source_resource_incarnation_id = o.source_resource_incarnation_id
          AND dpa.provider_account_id = o.payload->>'accountId'
        WHERE o.deployment_id = ${deploymentId}
          AND o.local_owner_id = ${subject}
          AND o.source_resource_incarnation_id = ${request.sourceResourceIncarnationId}::uuid
          AND o.provider_version = ${request.providerVersion}
          AND o.semantic_hash = ${request.semanticHash}
          AND o.hash_specification_version = ${request.hashSpecificationVersion}
        ORDER BY q.created_at DESC, o.recorded_at DESC, o.observation_id DESC
      `
      if (rows.length !== 1) throw new InvestecCardCurrencyCorrectionError(404, 'exact quarantined card observation was not found')
      const source = rows[0]
      const normalized = record(source.payload)
      const providerAccountId = stringValue(source.provider_account_id)
      const accountCurrency = stringValue(source.account_currency)
      if (!normalized || !providerAccountId || !accountCurrency) {
        throw new InvestecCardCurrencyCorrectionError(422, 'exact card observation lacks admitted account evidence')
      }
      const connections = await deps.loadConnections(deploymentId)
      const connection = connectionFor(connections, providerAccountId, request.sourceResourceIncarnationId)
      const patchedPayload: InvestecCardPayload = {
        ...providerPayload(source.raw_payload, normalized),
        centsAmount,
        currencyCode: request.currency,
      }
      const observation = investecCardObservation({
        deploymentId,
        localOwnerId: subject,
        sourceResourceIncarnationId: request.sourceResourceIncarnationId,
        providerAccountId,
        accountCurrency,
        payload: patchedPayload,
        receivedAt: source.observed_at,
      })
      if (!observation || observation.providerVersion !== request.providerVersion) {
        throw new InvestecCardCurrencyCorrectionError(422, 'owner correction could not preserve the provider card identity')
      }
      const auditEnvelope = sanitizeInvestecCardEnvelope({
        schemaVersion: 1,
        eventType: 'after_transaction',
        providerEventTime: observation.observedAt,
        transaction: patchedPayload,
        ownerCorrection: {
          kind: 'INVESTEC_CARD_CURRENCY_AMOUNT_OWNER_AUTHORIZED',
          actorSubject: subject,
          reason: request.reason,
          sourceObservationId: source.observation_id,
          sourceQuarantineId: source.quarantine_id,
          sourceSemanticHash: request.semanticHash,
          sourceHashSpecificationVersion: request.hashSpecificationVersion,
          correctedAmount: request.amount,
          correctedCurrency: request.currency,
        },
      })
      const corrected = { ...observation, rawPayload: auditEnvelope }
      const inserted = await deps.persistAndEnqueue(connection, [corrected], { skipCurrentState: true })
      const projection = await deps.projectPending(connection)
      if (projection.failed > 0) throw new InvestecCardCurrencyCorrectionError(503, 'owner correction projection is unresolved')
      const transactionId = stringValue(normalized.transactionId)
      if (!transactionId) throw new InvestecCardCurrencyCorrectionError(422, 'exact card transaction identity is missing')
      return {
        correctionId: correctionId(request),
        observationId: source.observation_id,
        quarantineId: source.quarantine_id,
        resourceId: `${deploymentId}:${request.sourceResourceIncarnationId}:${transactionId}`,
        providerVersion: observation.providerVersion,
        semanticHash: observation.semanticHash,
        inserted: inserted > 0,
        delivered: projection.delivered,
      }
    },
  }
}
