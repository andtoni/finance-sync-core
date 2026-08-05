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

const HASH_SPECIFICATION_VERSION = 1

function record(value: unknown): RecordValue | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function sha256(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex')
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
  return `-${whole}.${fraction}`
}

function cardAmount(payload: InvestecCardPayload): string | undefined {
  if (payload.centsAmount !== undefined) return decimalFromCents(payload.centsAmount)
  const value = typeof payload.amount === 'number' && Number.isFinite(payload.amount)
    ? payload.amount.toFixed(2)
    : typeof payload.amount === 'string' && /^[+-]?\d+(?:\.\d+)?$/.test(payload.amount.trim())
      ? payload.amount.trim()
      : undefined
  if (!value || Number(value) === 0) return undefined
  const magnitude = value.replace(/^[+-]/, '')
  return String(payload.type).toUpperCase() === 'CREDIT' || value.startsWith('-') ? magnitude : `-${magnitude}`
}

function merchantName(payload: InvestecCardPayload): string {
  const merchant = record(payload.merchant)
  return stringValue(merchant?.name) ?? 'Card purchase'
}

function cardNumber(payload: InvestecCardPayload): string | null {
  const card = record(payload.card)
  return stringValue(card?.display) ?? null
}

function observedAt(payload: InvestecCardPayload, receivedAt: unknown): string {
  const candidate = stringValue(payload.dateTime) ?? stringValue(receivedAt)
  if (candidate && Number.isFinite(Date.parse(candidate))) return new Date(candidate).toISOString()
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
  const amount = cardAmount(input.payload)
  const dateTime = observedAt(input.payload, input.receivedAt)
  const date = dateTime.slice(0, 10)
  const merchant = merchantName(input.payload)
  const reference = stringValue(input.payload.reference)
  if (!amount || !date || !/^[A-Z]{3}$/.test(input.accountCurrency)) return null
  const identity = reference
    ? `reference-${reference}`
    : `fingerprint-${sha256([input.providerAccountId, dateTime, amount, merchant].join('\x00')).slice(0, 32)}`
  const transactionId = `investec-card:${input.providerAccountId}:${identity}`
  const currency = input.accountCurrency.toUpperCase()
  const payload = Object.freeze({
    transactionId,
    accountId: input.providerAccountId,
    amount,
    currency,
    date,
    description: merchant,
    transactionType: 'CARD',
    cardNumber: cardNumber(input.payload),
    reference: reference ?? null,
    status: 'pending',
    merchantContext: merchantContext({
      provider: 'investec', rawDescription: merchant, merchantDisplay: merchant,
      amount, providerHint: reference ? { reference } : null,
    }),
  })
  const semanticHash = sha256([
    input.deploymentId, input.localOwnerId, input.sourceResourceIncarnationId,
    'cash_ledger', transactionId, amount, currency, dateTime, merchant, 'PENDING',
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
  await deps.persistAndEnqueue(connection, [observation])
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
