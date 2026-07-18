
import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'

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
  getBalance(accountId: string): Promise<{ currency?: string; currentBalance?: number; availableBalance?: number }>
  getTransactionPage(accountId: string, window: { fromDate: string; toDate: string }, cursor?: string): Promise<{
    transactions: readonly { uuid?: string; status?: string; postedOrder?: number; type?: string; amount?: number; postingDate?: string; transactionDate?: string; valueDate?: string; description?: string; cardNumber?: string }[]
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

  for (const account of allAccounts) {
    if (admittedAccounts && !admittedAccounts.has(account.accountId)) continue
    if (!account.productName || !eligibleSet.has(account.productName)) continue
    const accountRef = redact(account.accountId)
    const balance = await transport.getBalance(account.accountId)
    const rawCurrency = balance.currency?.trim().toUpperCase()
    if (!rawCurrency || !/^[A-Z]{3}$/.test(rawCurrency)) {
      quarantine.push({ reason: 'MISSING_ACCOUNT_CURRENCY', accountRef })
      continue
    }
    const displayName = account.accountName ?? account.productName ?? accountRef
    accounts.push({ accountRef, eligible: true, currency: rawCurrency, displayName })

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

          const abs = Math.abs(Number(txn.amount ?? 0))
          const signed = (txn.type ?? '').toUpperCase() === 'DEBIT' && abs > 0 ? -abs : abs

          const payload = {
            transactionId: txn.uuid,
            accountId: account.accountId,
            accountRef: redact(account.accountId),
            amount: signed.toFixed(2),
            currency: rawCurrency,
            date,
            description: txn.description ?? null,
            transactionType: txn.type ?? null,
            cardNumber: txn.cardNumber ? redact(txn.cardNumber) : null,
          }
          const semanticStr = [config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'cash_ledger', txn.uuid, signed.toFixed(2), rawCurrency, date].join('\x00')
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
