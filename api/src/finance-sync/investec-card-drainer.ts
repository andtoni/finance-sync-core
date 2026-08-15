import { createHash } from 'node:crypto'
import { startDrainer, type DrainerConfig, type DrainerHandle, type PulledMessage } from '../drain/drainer'
import { merchantContext } from './probes/shared/merchant-context'
import type { ObservationOutput } from './probes/investec/investec-adapter'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'
import { readExclusiveInput } from './secret-input'

interface RecordValue { readonly [key: string]: unknown }

export interface InvestecCardPayload extends RecordValue {
  readonly accountNumber?: unknown
  readonly centsAmount?: unknown
  readonly amount?: unknown
  readonly type?: unknown
  readonly currencyCode?: unknown
  readonly dateTime?: unknown
  readonly reference?: unknown
  readonly merchant?: unknown
  readonly card?: unknown
}

interface QueueEnvelope extends RecordValue {
  readonly source?: unknown
  readonly payload?: unknown
  readonly received_at?: unknown
}

interface CardAccountMapping {
  readonly local_owner_id: string
  readonly connection_id: string
  readonly provider_account_id: string
  readonly wealthfolio_account_id: string
  readonly source_resource_incarnation_id: string
  readonly currency: string
  readonly mask: string
}

const HASH_SPECIFICATION_VERSION = 4

function record(value: unknown): RecordValue | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function sha256(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex')
}

function providerLifecycleTransactionIds(payload: RecordValue): readonly string[] {
  const ids = new Set<string>()
  for (const key of [
    'providerLifecycleTransactionId', 'pendingTransactionId', 'postedTransactionId', 'settlementTransactionId',
    'successorTransactionId', 'cardTransactionId', 'linkedTransactionId',
  ]) {
    const value = payload[key]
    if (typeof value === 'string' && value.trim()) ids.add(value.trim())
  }
  return [...ids]
}

function providerLifecycleTransactionId(payload: RecordValue): string | undefined {
  const ids = providerLifecycleTransactionIds(payload)
  return ids.length === 1 ? ids[0] : undefined
}

function hasConflictingLifecycleIds(payload: RecordValue): boolean {
  return providerLifecycleTransactionIds(payload).length > 1
}

function accountMask(value: unknown): string | undefined {
  const raw = stringValue(value)
  if (!raw) return undefined
  const digits = raw.replace(/\s+/g, '')
  return digits.length >= 4 ? digits.slice(-4) : undefined
}

function decimalFromCents(value: unknown): string | undefined {
  const raw = typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
    ? String(value)
    : typeof value === 'string' && /^\d+$/.test(value.trim()) ? value.trim() : undefined
  if (!raw || /^0+$/.test(raw)) return undefined
  const whole = raw.length > 2 ? raw.slice(0, -2) : '0'
  const fraction = raw.slice(-2).padStart(2, '0')
  return `${whole}.${fraction}`
}

function cardCredit(payload: InvestecCardPayload): boolean {
  if (String(payload.type).toUpperCase() === 'CREDIT') return true
  const rawAmount = payload.amount
  if (typeof rawAmount === 'number') return Number.isFinite(rawAmount) && rawAmount < 0
  return typeof rawAmount === 'string' && rawAmount.trim().startsWith('-')
}

function signedAmount(value: unknown, isCredit: boolean): string | undefined {
  const magnitude = decimalFromCents(value)
  if (!magnitude) return undefined
  return isCredit ? magnitude : `-${magnitude}`
}

function merchantName(payload: InvestecCardPayload): string {
  const merchant = record(payload.merchant)
  return stringValue(merchant?.name) ?? stringValue(payload.merchant) ?? 'Card purchase'
}

function cardNumber(payload: InvestecCardPayload): string | null {
  const card = record(payload.card)
  return stringValue(card?.display) ?? null
}

function observedAt(payload: InvestecCardPayload, receivedAt: unknown): string {
  for (const candidate of [stringValue(payload.dateTime), stringValue(receivedAt)]) {
    if (candidate && Number.isFinite(Date.parse(candidate))) return new Date(candidate).toISOString()
  }
  return new Date().toISOString()
}

/**
 * Convert an Investec afterTransaction card fact into the canonical observation
 * shape. The account number is resolved separately by exact last-four admission;
 * this function never guesses an account or links to a posted row.
 */
export function investecCardObservation(input: {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly providerAccountId: string
  readonly accountCurrency: string
  readonly payload: InvestecCardPayload
  readonly receivedAt?: string
}): ObservationOutput | null {
  const isCredit = cardCredit(input.payload)
  const providerDateTime = stringValue(input.payload.dateTime)
  const hasProviderDateTime = providerDateTime !== undefined && Number.isFinite(Date.parse(providerDateTime))
  const dateTime = observedAt(input.payload, input.receivedAt)
  const date = dateTime.slice(0, 10)
  const merchant = merchantName(input.payload)
  const reference = stringValue(input.payload.reference)
  if (hasConflictingLifecycleIds(input.payload)) return null
  const lifecycleTransactionId = providerLifecycleTransactionId(input.payload)
  const accountCurrency = stringValue(input.accountCurrency)?.toUpperCase() ?? ''
  const authorizationCurrency = stringValue(input.payload.currencyCode)?.toUpperCase()
  // centsAmount and currencyCode are a single provider-described authorization
  // leg. Never substitute the admitted account currency for missing evidence.
  const amount = signedAmount(input.payload.centsAmount, isCredit)
  if (!amount || !date || !/^[A-Z]{3}$/.test(accountCurrency)
    || !authorizationCurrency || !/^[A-Z]{3}$/.test(authorizationCurrency)) return null
  const identity = reference
    ? `reference-${reference}`
    : `fingerprint-${sha256([input.providerAccountId, dateTime, amount, merchant].join('\x00')).slice(0, 32)}`
  const transactionId = `investec-card:${input.providerAccountId}:${identity}`
  const cardDisplay = cardNumber(input.payload)
  const originalCurrency = authorizationCurrency
  const originalAmount = decimalFromCents(input.payload.centsAmount)
  if (!originalAmount) return null
  const currencyEvidenceStatus = 'provider_explicit'
  const payload = Object.freeze({
    transactionId,
    accountId: input.providerAccountId,
    amount,
    currency: originalCurrency,
    originalAmount,
    originalCurrency,
    ...(hasProviderDateTime ? { transactionDate: date } : {}),
    date,
    authorizationDateTime: dateTime,
    description: merchant,
    sourceType: 'CARD',
    subtype: 'CARD_PURCHASE',
    type: isCredit ? 'CREDIT' : 'DEBIT',
    direction: isCredit ? 'CREDIT' : 'DEBIT',
    transactionType: 'CARD',
    cardNumber: cardDisplay,
    reference: reference ?? null,
    ...(lifecycleTransactionId === undefined ? {} : { providerLifecycleTransactionId: lifecycleTransactionId }),
    status: 'pending',
    metadata: {
      providerOwned: {
        investec: {
          accountCurrency,
          cardCurrency: originalCurrency,
          cardNumber: cardDisplay,
          currencyEvidence: {
            accountCurrency,
            originalCurrency,
            originalAmount,
            status: currencyEvidenceStatus,
            settlementCurrency: null,
          },
        },
      },
    },
    merchantContext: merchantContext({
      provider: 'investec', rawDescription: merchant, merchantDisplay: merchant,
      amount,
      providerHint: {
        ...(reference ? { reference } : {}),
        currency: originalCurrency,
      },
    }),
  })
  // A provider reference is the card event identity. Redelivery may carry a
  // different received_at timestamp, but a newly supplied provider dateTime is
  // a canonical correction and must advance the observation hash.
  const semanticHash = sha256([
    input.deploymentId, input.localOwnerId, input.sourceResourceIncarnationId,
    'cash_ledger', transactionId, amount, accountCurrency, originalAmount ?? '', originalCurrency ?? '',
    currencyEvidenceStatus, reference ? (hasProviderDateTime ? providerDateTime : '') : dateTime,
    merchant, cardDisplay ?? '', 'PENDING', lifecycleTransactionId ?? '',
  ].join('\x00'))
  return Object.freeze({
    deploymentId: input.deploymentId,
    localOwnerId: input.localOwnerId,
    sourceResourceIncarnationId: input.sourceResourceIncarnationId,
    providerVersion: `investec:card:${transactionId}`,
    semanticHash,
    hashSpecificationVersion: HASH_SPECIFICATION_VERSION,
    domain: 'cash_ledger',
    payload,
    rawPayload: input.payload,
    observedAt: dateTime,
  })
}

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
  readonly semantic_hash: string
  readonly hash_specification_version: number
  readonly observed_at: string | Date
  readonly source_resource_incarnation_id: string
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
      SELECT DISTINCT ON (source_resource_incarnation_id, payload->>'transactionId')
        payload, raw_payload, semantic_hash, hash_specification_version, observed_at, source_resource_incarnation_id::text
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND provider_version LIKE 'investec:card:%'
        AND raw_payload IS NOT NULL
      ORDER BY source_resource_incarnation_id, payload->>'transactionId', recorded_at DESC, observation_id DESC
    )
    SELECT payload, raw_payload, semantic_hash, hash_specification_version, observed_at, source_resource_incarnation_id
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
    const rawPayload = record(row.raw_payload)
    if (!providerAccountId || !rawPayload) {
      unresolved = true
      continue
    }
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
      && row.hash_specification_version === HASH_SPECIFICATION_VERSION) continue
    const identity = `${connection.connectionId}:${observation.semanticHash}`
    if (seen.has(identity)) {
      unresolved = true
      continue
    }
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

async function handleInvestecCardMessage(
  sql: FinanceSyncSql,
  deploymentId: string,
  deps: SyncRuntimeDependencies,
  message: PulledMessage,
  connections: readonly RuntimeConnection[],
): Promise<boolean> {
  const envelope = record(message.envelope) as QueueEnvelope | undefined
  if (envelope?.source !== 'investec') return false
  const payload = record(envelope.payload) as InvestecCardPayload | undefined
  const mask = accountMask(payload?.accountNumber)
  if (!payload || !mask) return true
  const observationMapping = await exactCardAccount(sql, deploymentId, mask)
  const connectionId = `${observationMapping.connection_id}:${observationMapping.provider_account_id}`
  const connection = connections.find(candidate => candidate.provider === 'investec' && candidate.connectionId === connectionId)
  if (!connection) throw new Error('Investec card account is not an active runtime connection')
  const observation = investecCardObservation({
    deploymentId,
    localOwnerId: connection.localOwnerId,
    sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
    providerAccountId: observationMapping.provider_account_id,
    accountCurrency: observationMapping.currency,
    payload,
    receivedAt: stringValue(envelope.received_at),
  })
  if (!observation) return true
  await deps.persistAndEnqueue(connection, [observation], { skipCurrentState: true })
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
  let connectionsPromise: Promise<readonly RuntimeConnection[]> | undefined
  const connections = () => connectionsPromise ??= deps.loadConnections(deploymentId)
  return startDrainer(config, {
    fetchImpl: fetch,
    handle: async message => handleInvestecCardMessage(sql, deploymentId, deps, message, await connections()),
    log: level => { if (level === 'warn' || level === 'error') console.error('FINANCE_SYNC_WEBHOOK_DRAIN_RETRY') },
  })
}
