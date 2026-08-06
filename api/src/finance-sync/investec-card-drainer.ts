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

const HASH_SPECIFICATION_VERSION = 3

function record(value: unknown): RecordValue | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function sha256(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex')
}

function providerLifecycleTransactionId(payload: RecordValue): string | undefined {
  for (const key of [
    'providerLifecycleTransactionId', 'pendingTransactionId', 'postedTransactionId', 'settlementTransactionId',
    'successorTransactionId', 'cardTransactionId', 'linkedTransactionId',
  ]) {
    const value = payload[key]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
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

function decimalFromValue(value: unknown): string | undefined {
  const raw = typeof value === 'number' && Number.isFinite(value)
    ? value.toFixed(2)
    : typeof value === 'string' && /^[+-]?\d+(?:\.\d+)?$/.test(value.trim())
      ? value.trim()
      : undefined
  if (!raw || Number(raw) === 0) return undefined
  return raw.replace(/^[+-]/, '')
}

function cardCredit(payload: InvestecCardPayload): boolean {
  if (String(payload.type).toUpperCase() === 'CREDIT') return true
  const rawAmount = payload.amount
  if (typeof rawAmount === 'number') return Number.isFinite(rawAmount) && rawAmount < 0
  return typeof rawAmount === 'string' && rawAmount.trim().startsWith('-')
}

function signedAmount(value: unknown, isCredit: boolean, cents: boolean): string | undefined {
  const magnitude = cents ? decimalFromCents(value) : decimalFromValue(value)
  if (!magnitude) return undefined
  return isCredit ? magnitude : `-${magnitude}`
}

function cardAmount(payload: InvestecCardPayload): string | undefined {
  const isCredit = cardCredit(payload)
  // Investec's centsAmount/amount is the debit to the admitted account. The
  // merchant currency is a separate evidence leg and must not relabel it.
  return signedAmount(payload.centsAmount, isCredit, true)
    ?? signedAmount(payload.amount, isCredit, false)
}

function cardOriginalAmount(payload: InvestecCardPayload, isCredit: boolean): string | undefined {
  for (const key of ['originalCentsAmount', 'foreignCentsAmount']) {
    const amount = signedAmount(payload[key], isCredit, true)
    if (amount !== undefined) return amount
  }
  for (const key of ['originalAmount', 'foreignAmount']) {
    const amount = signedAmount(payload[key], isCredit, false)
    if (amount !== undefined) return amount
  }
  return undefined
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
  const isCredit = cardCredit(input.payload)
  const amount = cardAmount(input.payload)
  const providerDateTime = stringValue(input.payload.dateTime)
  const dateTime = observedAt(input.payload, input.receivedAt)
  const date = dateTime.slice(0, 10)
  const merchant = merchantName(input.payload)
  const reference = stringValue(input.payload.reference)
  const lifecycleTransactionId = providerLifecycleTransactionId(input.payload)
  if (!amount || !date || !/^[A-Z]{3}$/.test(input.accountCurrency)) return null
  const identity = reference
    ? `reference-${reference}`
    : `fingerprint-${sha256([input.providerAccountId, dateTime, amount, merchant].join('\x00')).slice(0, 32)}`
  const transactionId = `investec-card:${input.providerAccountId}:${identity}`
  const accountCurrency = input.accountCurrency.toUpperCase()
  const originalCurrency = (stringValue(input.payload.currencyCode) ?? accountCurrency).toUpperCase()
  if (!/^[A-Z]{3}$/.test(originalCurrency)) return null
  const cardDisplay = cardNumber(input.payload)
  const providerOriginalAmount = cardOriginalAmount(input.payload, isCredit)
  const originalAmount = providerOriginalAmount ?? (originalCurrency === accountCurrency ? amount : undefined)
  const payload = Object.freeze({
    transactionId,
    accountId: input.providerAccountId,
    amount,
    currency: accountCurrency,
    ...(originalAmount === undefined ? {} : { originalAmount }),
    originalCurrency,
    settlementCurrency: accountCurrency,
    date,
    transactionDate: date,
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
          cardCurrency: originalCurrency,
          accountCurrency,
          cardNumber: cardDisplay,
        },
      },
    },
    merchantContext: merchantContext({
      provider: 'investec', rawDescription: merchant, merchantDisplay: merchant,
      amount, providerHint: { ...(reference ? { reference } : {}), currency: originalCurrency },
    }),
  })
  // A provider reference is the card event identity. Redelivery may carry a
  // different received_at timestamp, but a newly supplied provider dateTime is
  // a canonical correction and must advance the observation hash.
  const semanticHash = sha256([
    input.deploymentId, input.localOwnerId, input.sourceResourceIncarnationId,
    'cash_ledger', transactionId, amount, accountCurrency, originalAmount ?? '', originalCurrency,
    reference ? (providerDateTime ?? '') : dateTime, merchant, cardDisplay ?? '', 'PENDING', lifecycleTransactionId ?? '',
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
