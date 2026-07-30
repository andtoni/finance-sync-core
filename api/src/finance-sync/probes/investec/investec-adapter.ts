
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

export interface InvestecTransport {
  listAccounts(): Promise<readonly { accountId: string; productName?: string; accountName?: string; accountNumber?: string; referenceName?: string }[]>
  getBalance(accountId: string): Promise<{ currency?: string; currentBalance?: string | number; availableBalance?: string | number }>
  getTransactionPage(accountId: string, window: { fromDate: string; toDate: string }, cursor?: string): Promise<{
    transactions: readonly { uuid?: string; status?: string; postedOrder?: number; type?: string; amount?: string | number; postingDate?: string; transactionDate?: string; valueDate?: string; description?: string; reference?: string; cardNumber?: string }[]
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

const HASH_SPEC_VERSION = 1

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
          if (txn.postedOrder === 0) {
            quarantine.push({ reason: 'POSTED_ORDER_ZERO_PROVISIONAL', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : undefined })
            continue
          }
          const status = (txn.status ?? '').toUpperCase()
          if (status !== 'POSTED') {
            if (status !== 'PENDING') {
              quarantine.push({ reason: 'UNKNOWN_OR_UNBOOKED_STATUS', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : undefined })
            }
            continue
          }
          const date = txn.postingDate ?? txn.transactionDate ?? txn.valueDate
          if (!date) { quarantine.push({ reason: 'MISSING_DATE', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : undefined }); continue }
          if (!txn.uuid) { quarantine.push({ reason: 'MISSING_IDENTITY', accountRef }); continue }

          const rawAmount = investecDecimal(txn.amount)
          if (rawAmount === null) { quarantine.push({ reason: 'INVALID_AMOUNT', accountRef, transactionRef: redact(txn.uuid) }); continue }
          const magnitude = rawAmount.replace(/^[+-]/, '')
          const isZero = /^0+(?:\.0+)?$/.test(magnitude)
          const signed = (txn.type ?? '').toUpperCase() === 'DEBIT' && !isZero ? `-${magnitude}` : magnitude

          const payload = {
            transactionId: txn.uuid,
            accountId: account.accountId,
            accountRef: redact(account.accountId),
            amount: signed,
            currency: rawCurrency,
            date,
            description: txn.description ?? null,
            transactionType: txn.type ?? null,
            cardNumber: txn.cardNumber ? redact(txn.cardNumber) : null,
            reference: txn.reference ?? null,
            merchantContext: merchantContext({
              provider: 'investec', rawDescription: txn.description ?? null,
              merchantDisplay: txn.description ?? txn.reference ?? null, amount: signed.startsWith('-') ? -1 : 1,
              providerHint: txn.reference ? { reference: txn.reference } : null,
            }),
          }
          const semanticStr = [config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'cash_ledger', txn.uuid, signed, rawCurrency, date].join('\x00')
          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId: config.sourceResourceIncarnationId,
            providerVersion: 'investec:txn:' + txn.uuid,
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
