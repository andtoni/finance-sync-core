
import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'
import { merchantContext } from '../shared/merchant-context'

export function investecResourceKind(): CanonicalDomain {
  return 'cash_ledger'
}

export interface ObservationOutput {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
  readonly domain: CanonicalDomain
  readonly payload: Readonly<Record<string, unknown>>
  readonly observedAt: string
}

interface InvestecTransaction {
  readonly uuid?: string
  readonly status?: string
  readonly postedOrder?: number
  readonly type?: string
  readonly transactionType?: string
  readonly amount?: string | number
  readonly postingDate?: string
  readonly actionDate?: string
  readonly transactionDate?: string
  readonly valueDate?: string
  readonly description?: string
  readonly reference?: string
  readonly cardNumber?: string
  readonly runningBalance?: string | number
}

export interface InvestecTransport {
  listAccounts(): Promise<readonly { accountId: string; productName?: string; accountName?: string; accountNumber?: string; referenceName?: string }[]>
  getBalance(accountId: string): Promise<{ currency?: string; currentBalance?: string | number; availableBalance?: string | number }>
  getTransactionPage(accountId: string, window: { fromDate: string; toDate: string }, cursor?: string): Promise<{
    transactions: readonly InvestecTransaction[]
    nextCursor?: string
  }>
}

export interface InvestecAdapterConfig {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly fromDate: string
  readonly toDate: string
  readonly eligibleProductNames: readonly string[]
  readonly accountIds?: readonly string[]
  readonly maxPages?: number
}

export interface InvestecDiscoveredAccount {
  readonly providerAccountId: string
  readonly name: string
  readonly type: string
  readonly currency: string
  readonly mask?: string
}

/** Strictly normalize the documented account-list and balance responses for onboarding. */
export async function discoverInvestecAccounts(
  transport: InvestecTransport,
  eligibleProductNames: readonly string[],
): Promise<readonly InvestecDiscoveredAccount[]> {
  const eligible = new Set(eligibleProductNames)
  const discovered: InvestecDiscoveredAccount[] = []
  for (const account of await transport.listAccounts()) {
    if (typeof account.accountId !== 'string' || !account.accountId
      || typeof account.productName !== 'string' || !eligible.has(account.productName)) continue
    const balance = await transport.getBalance(account.accountId)
    const currency = balance.currency?.trim().toUpperCase()
    const name = account.accountName ?? account.referenceName ?? account.productName
    if (!name || !currency || !/^[A-Z]{3}$/.test(currency)) throw new Error('invalid Investec account discovery response')
    const mask = account.accountNumber?.slice(-4)
    discovered.push({
      providerAccountId: account.accountId,
      name,
      type: account.productName,
      currency,
      ...(mask ? { mask } : {}),
    })
  }
  return Object.freeze(discovered)
}

export interface InvestecAdapterResult {
  readonly accounts: readonly { accountRef: string; eligible: boolean; currency: string; displayName: string }[]
  readonly observations: readonly ObservationOutput[]
  readonly quarantine: readonly { reason: string; accountRef: string; transactionRef?: string }[]
  readonly pagesPolled: number
  readonly paginationComplete: boolean
  readonly nextCursor: string | null
}

const HASH_SPEC_VERSION = 2
const PRIME_SAVER_IDENTITY_VERSION = 2

function sha256(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

function redact(value: string): string {
  if (value.length < 4) return value
  return '\u2026' + value.slice(-4)
}

function investecDecimal(value: unknown): string | null {
  if (typeof value !== 'string' && (typeof value !== 'number' || !Number.isFinite(value))) return null
  const trimmed = String(value).trim()
  return /^[+-]?\d+(?:\.\d+)?$/.test(trimmed) ? trimmed : null
}

/**
 * PrimeSaver returns booked rows with postedOrder=0 and an empty uuid. Those
 * rows are valid, but unlike Private Bank transactions they have no upstream
 * identity. Keep the identity tied to the provider's stable transaction
 * reference (usually the MBP/UBP reference embedded in the narrative), or to
 * the immutable-looking economic fields when no reference is available.
 *
 * Deliberately excluded: postingDate, actionDate, valueDate and runningBalance.
 * Investec revises those fields while retaining the same booked row; including
 * them creates a new resource ID and re-projects a duplicate activity.
 */
function identityText(value: unknown): string {
  return typeof value === 'string' ? value.trim().replace(/\s+/g, ' ').toUpperCase() : ''
}

function stableProviderReference(reference: unknown, description: unknown): string | null {
  for (const candidate of [reference, description]) {
    const text = identityText(candidate)
    if (!text) continue
    const embedded = text.match(/\b(?:MBP|UBP)\d{6,}\b/)
    if (embedded) return embedded[0]
    if (candidate === reference) return text
  }
  return null
}

interface PrimeSaverIdentity {
  readonly transactionId: string
  readonly identityKey: string
  readonly strongReference: boolean
}

function primeSaverIdentity(
  accountId: string,
  amount: unknown,
  type: unknown,
  transactionType: unknown,
  description: unknown,
  reference: unknown,
  cardNumber: unknown,
  date: unknown,
): PrimeSaverIdentity | null {
  const normalizedAmount = investecDecimal(amount)
  if (!normalizedAmount) return null
  const providerReference = stableProviderReference(reference, description)
  const strongReference = providerReference !== null
  const fingerprint = strongReference
    ? [
      `prime-saver-identity-v${PRIME_SAVER_IDENTITY_VERSION}`,
      'reference', accountId, providerReference,
    ]
    : [
      `prime-saver-identity-v${PRIME_SAVER_IDENTITY_VERSION}`,
      'fallback', accountId, normalizedAmount,
      identityText(type), identityText(transactionType), identityText(description),
      identityText(cardNumber), typeof date === 'string' ? date.trim() : '',
    ]
  const identityKey = `v${PRIME_SAVER_IDENTITY_VERSION}:${sha256(JSON.stringify(fingerprint))}`
  return {
    transactionId: `synthetic-${identityKey}`,
    identityKey,
    strongReference,
  }
}

/**
 * Compute the exact normalized key used only to discover legacy resource IDs.
 * Unlike the resource identity, this key uses fields retained in normalized
 * observations and excludes the provider date and running balance. It is not
 * a fuzzy match: account, amount, narrative, type, and card identity must all
 * agree, while an embedded MBP/UBP reference remains the preferred key.
 */
export function investecPrimeSaverSupersessionKey(payload: Readonly<Record<string, unknown>>): string | null {
  const accountId = typeof payload.accountId === 'string' ? payload.accountId : ''
  const normalizedAmount = investecDecimal(payload.amount)
  if (!accountId || !normalizedAmount) return null
  const providerReference = stableProviderReference(payload.reference, payload.description)
  if (providerReference !== null) return `reference-v1:${accountId}:${providerReference}`
  const description = identityText(payload.description)
  if (!description) return null
  const fingerprint = [
    'prime-saver-supersession-v1',
    accountId,
    normalizedAmount,
    identityText(payload.transactionType),
    description,
    identityText(payload.cardNumber),
  ]
  return `fallback-v1:${sha256(JSON.stringify(fingerprint))}`
}

/** Compute the same PrimeSaver identity from a normalized observation payload. */
export function investecPrimeSaverIdentityKey(payload: Readonly<Record<string, unknown>>): string | null {
  const key = primeSaverIdentity(
    String(payload.accountId ?? ''),
    payload.amount,
    payload.transactionType,
    payload.transactionType,
    payload.description,
    payload.reference,
    payload.cardNumber,
    payload.date,
  )
  return key?.identityKey ?? null
}

function transactionIdentity(
  accountId: string,
  productName: string | undefined,
  transaction: InvestecTransaction,
): { transactionId: string; primeSaverIdentityKey?: string } | null {
  const uuid = transaction.uuid?.trim()
  if (uuid) return { transactionId: uuid }
  const normalizedProduct = productName?.replace(/\s+/g, '').toLowerCase()
  if (normalizedProduct !== 'primesaver' || transaction.postedOrder !== 0) return null
  const date = transaction.transactionDate ?? transaction.valueDate ?? transaction.postingDate
  const identity = primeSaverIdentity(
    accountId,
    transaction.amount,
    transaction.type,
    transaction.transactionType,
    transaction.description,
    transaction.reference,
    transaction.cardNumber,
    date,
  )
  if (!identity) return null
  return { transactionId: identity.transactionId, primeSaverIdentityKey: identity.identityKey }
}

export async function runInvestecAdapter(config: InvestecAdapterConfig, transport: InvestecTransport): Promise<InvestecAdapterResult> {
  const eligibleSet = new Set(config.eligibleProductNames)
  const admittedAccounts = config.accountIds ? new Set(config.accountIds) : undefined
  const allAccounts = await transport.listAccounts()
  const accounts: { accountRef: string; eligible: boolean; currency: string; displayName: string }[] = []
  const observations: ObservationOutput[] = []
  const quarantine: { reason: string; accountRef: string; transactionRef?: string }[] = []
  let pagesPolled = 0
  let paginationComplete = true
  let nextCursor: string | null = null
  const seenAccountIds = new Set<string>()

  for (const account of allAccounts) {
    if (seenAccountIds.has(account.accountId)) continue
    seenAccountIds.add(account.accountId)
    if (admittedAccounts) {
      if (!admittedAccounts.has(account.accountId)) continue
    } else if (!account.productName || !eligibleSet.has(account.productName)) {
      continue
    }
    const accountRef = redact(account.accountId)
    const balance = await transport.getBalance(account.accountId)
    // Capture the successful fetch instant. The configured transaction window
    // is not evidence of when this independently fetched current balance was observed.
    const balanceObservedAt = new Date().toISOString()
    const rawCurrency = balance.currency?.trim().toUpperCase()
    if (!rawCurrency || !/^[A-Z]{3}$/.test(rawCurrency)) {
      quarantine.push({ reason: 'MISSING_ACCOUNT_CURRENCY', accountRef })
      continue
    }
    const displayName = account.accountName ?? account.productName ?? accountRef
    accounts.push({ accountRef, eligible: true, currency: rawCurrency, displayName })
    const currentBalance = investecDecimal(balance.currentBalance)
    if (currentBalance === null) {
      quarantine.push({ reason: 'MISSING_CURRENT_BALANCE', accountRef })
      paginationComplete = false
      continue
    }
    const balancePayload = Object.freeze({
      accountId: account.accountId,
      currentBalance,
      currency: rawCurrency,
      asOfDate: config.toDate,
    })
    observations.push({
      deploymentId: config.deploymentId,
      localOwnerId: config.localOwnerId,
      sourceResourceIncarnationId: config.sourceResourceIncarnationId,
      providerVersion: `investec:balance:${account.accountId}`,
      semanticHash: sha256([config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'balance', JSON.stringify(balancePayload)].join('\x00')),
      hashSpecificationVersion: HASH_SPEC_VERSION,
      domain: 'balance',
      payload: balancePayload,
      observedAt: balanceObservedAt,
    })

    let cursor: string | undefined
    let accountComplete = false
    for (let page = 0; page < (config.maxPages ?? 50); page++) {
      try {
        const pageResult = await transport.getTransactionPage(account.accountId, { fromDate: config.fromDate, toDate: config.toDate }, cursor)
        pagesPolled++
        for (const txn of pageResult.transactions) {
          const status = (txn.status ?? '').toUpperCase()
          if (status !== 'POSTED' && status !== 'PENDING') {
            quarantine.push({ reason: 'UNKNOWN_OR_UNBOOKED_STATUS', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : undefined })
            continue
          }
          // A UUID-bearing postedOrder=0 row is still the provisional shape used
          // by the card/statement feed. PrimeSaver's booked rows have no UUID and
          // also use postedOrder=0, so transactionIdentity handles that valid case.
          if (status === 'POSTED' && txn.postedOrder === 0 && txn.uuid?.trim()) {
            quarantine.push({ reason: 'POSTED_ORDER_ZERO_PROVISIONAL', accountRef, transactionRef: redact(txn.uuid) })
            continue
          }
          // Investec's transactionDate is the date the transaction occurred; postingDate is
          // when it was entered into the account ledger and can be days later (PrimeSaver
          // exposes this as the poll/current posting date).
          const date = txn.transactionDate ?? txn.valueDate ?? txn.postingDate
          if (!date) { quarantine.push({ reason: 'MISSING_DATE', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : undefined }); continue }
          const identity = transactionIdentity(account.accountId, account.productName, txn)
          if (!identity) { quarantine.push({ reason: 'MISSING_IDENTITY', accountRef }); continue }
          const transactionId = identity.transactionId

          const rawAmount = investecDecimal(txn.amount)
          if (rawAmount === null) {
            quarantine.push({ reason: 'INVALID_AMOUNT', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : transactionId })
            continue
          }
          const magnitude = rawAmount.replace(/^[+-]/, '')
          const isZero = /^0+(?:\.0+)?$/.test(magnitude)
          const signed = (txn.type ?? '').toUpperCase() === 'DEBIT' && !isZero ? `-${magnitude}` : magnitude

          const payload = {
            transactionId,
            accountId: account.accountId,
            accountRef: redact(account.accountId),
            amount: signed,
            currency: rawCurrency,
            date,
            description: txn.description ?? null,
            transactionType: txn.type ?? null,
            cardNumber: txn.cardNumber ? redact(txn.cardNumber) : null,
            reference: txn.reference ?? null,
            status: status.toLowerCase(),
            ...(identity.primeSaverIdentityKey === undefined ? {} : { primeSaverIdentityKey: identity.primeSaverIdentityKey }),
            merchantContext: merchantContext({
              provider: 'investec', rawDescription: txn.description ?? null,
              merchantDisplay: txn.description ?? txn.reference ?? null, amount: signed.startsWith('-') ? -1 : 1,
              providerHint: txn.reference ? { reference: txn.reference } : null,
            }),
          }
          const semanticStr = [config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'cash_ledger', transactionId, signed, rawCurrency, date, status].join('\x00')
          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId: config.sourceResourceIncarnationId,
            providerVersion: 'investec:txn:' + transactionId,
            semanticHash: sha256(semanticStr),
            hashSpecificationVersion: HASH_SPEC_VERSION,
            domain: 'cash_ledger',
            payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
            observedAt: new Date(date).toISOString(),
          })
        }
        cursor = pageResult.nextCursor
        if (!cursor) { accountComplete = true; break }
        nextCursor = cursor
      } catch {
        paginationComplete = false
        break
      }
    }
    if (!accountComplete) paginationComplete = false
  }
  return Object.freeze({ accounts, observations, quarantine, pagesPolled, paginationComplete, nextCursor })
}
