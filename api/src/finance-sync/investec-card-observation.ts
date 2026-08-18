import { createHash } from 'node:crypto'
import { merchantContext } from './probes/shared/merchant-context'
import type { ObservationOutput } from './probes/investec/investec-adapter'

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

export function accountMask(value: unknown): string | undefined {
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

function strictProviderInstant(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const instant = value.trim()
  if (!/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(instant)) return undefined
  const date = instant.slice(0, 10)
  const calendar = new Date(`${date}T00:00:00Z`)
  if (!Number.isFinite(calendar.getTime()) || calendar.toISOString().slice(0, 10) !== date) return undefined
  const parsed = new Date(instant)
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : undefined
}

function cardNumber(payload: InvestecCardPayload): string | null {
  const card = record(payload.card)
  const display = stringValue(card?.display)
  if (!display) return null
  const digits = display.replace(/\D/g, '')
  if (digits.length < 4) return null
  return `••••${digits.slice(-4)}`
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
  /** Retained for historical replay provenance; never used as activity time. */
  readonly receivedAt?: string
}): ObservationOutput | null {
  const isCredit = cardCredit(input.payload)
  const dateTime = strictProviderInstant(input.payload.dateTime)
  if (!dateTime) return null
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
    transactionDate: date,
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
          ...(!isCredit ? { provisionalSpending: true, provisionalSpendingReason: 'AUTHORIZED_NO_SETTLEMENT' } : {}),
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
    currencyEvidenceStatus, 'provisional-spending-v1', dateTime,
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
