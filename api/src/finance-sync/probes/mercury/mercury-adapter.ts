import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'

export function mercuryResourceKind(mercuryType: string): CanonicalDomain {
  switch (mercuryType) {
    case 'cash': case 'depository': return 'cash_ledger'
    case 'credit': case 'loan': return 'liability'
    case 'investment': return 'brokerage_activity'
    default: return 'cash_ledger'
  }
}

export interface MercuryAccount {
  readonly id: string
  readonly name: string
  readonly type: string
  readonly currentBalance: number | null
  readonly availableBalance: number | null
  readonly currency: string
  readonly status: string
}

export interface MercuryTransaction {
  readonly id: string
  readonly accountId: string
  readonly amount: number
  readonly counterpartyName: string | null
  readonly bankDescription: string | null
  readonly note: string | null
  readonly createdAt: string | null
  readonly postedAt: string | null
  readonly status: string
  readonly currency: string | null
}

export interface MercuryAccountPage {
  readonly accounts: readonly MercuryAccount[]
}

export interface MercuryTransactionPage {
  readonly transactions: readonly MercuryTransaction[]
  readonly total: number
}

export interface MercuryTransport {
  fetchAccounts(): Promise<MercuryAccountPage>
  fetchTransactions(accountId: string, offset: number, limit: number, since?: string): Promise<MercuryTransactionPage>
}

export interface MercurySyncConfig {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly accountIds?: readonly string[]
  readonly since?: string
  readonly maxPagesPerAccount?: number
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

export interface MercurySyncResult {
  readonly accounts: readonly MercuryAccount[]
  readonly observations: readonly ObservationOutput[]
  readonly accountsFetched: number
  readonly pagesFetched: number
  readonly transactionsObserved: number
  readonly quarantinedTransactions: number
  readonly paginationComplete: boolean
  /** Provider-observed high-water mark with a safety overlap, never wall clock. */
  readonly nextSince: string | null
}

const HASH_SPEC_VERSION = 1
const PAGE_LIMIT = 500
const MAX_PAGES_PER_ACCOUNT = 50
const CHECKPOINT_OVERLAP_MS = 24 * 60 * 60 * 1000

function sha256(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

function decimalString(value: number | null | undefined): string {
  if (value === null || value === undefined) return '0.00'
  if (!Number.isFinite(value)) return '0.00'
  return value.toFixed(2)
}

function toIsoDate(raw: string | null | undefined): string {
  if (!raw) return new Date().toISOString()
  try {
    const d = new Date(raw)
    return Number.isFinite(d.getTime()) ? d.toISOString() : new Date().toISOString()
  } catch {
    return new Date().toISOString()
  }
}

function ledgerStatus(status: string): 'posted' | 'pending' | null {
  switch (status) {
    case 'sent': case 'posted': return 'posted'
    case 'pending': return 'pending'
    default: return null
  }
}

export async function runMercurySync(
  config: MercurySyncConfig,
  transport: MercuryTransport,
): Promise<MercurySyncResult> {
  const observations: ObservationOutput[] = []
  let accountsFetched = 0
  let pagesFetched = 0
  let transactionsObserved = 0
  let quarantinedTransactions = 0
  let latestProviderTimestamp: number | null = null
  let paginationComplete = true

  let allAccounts: MercuryAccount[]
  try {
    const accountPage = await transport.fetchAccounts()
    allAccounts = [...accountPage.accounts]
    accountsFetched = allAccounts.length
  } catch {
    return { accounts: [], observations, accountsFetched: 0, pagesFetched: 0, transactionsObserved: 0, quarantinedTransactions: 0, paginationComplete: false, nextSince: null }
  }

  if (config.accountIds && config.accountIds.length > 0) {
    const idSet = new Set(config.accountIds)
    allAccounts = allAccounts.filter(a => idSet.has(a.id))
  }

  const maxPages = config.maxPagesPerAccount ?? MAX_PAGES_PER_ACCOUNT

  for (const account of allAccounts) {
    const balanceDomain = mercuryResourceKind(account.type)
    const balancePayload = {
      accountId: account.id,
      accountName: account.name,
      accountType: account.type,
      currentBalance: decimalString(account.currentBalance),
      availableBalance: decimalString(account.availableBalance),
      currency: account.currency,
      status: account.status,
    }
    const balanceSemantic = [
      config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
      balanceDomain, 'balance', account.id, decimalString(account.currentBalance), account.currency,
    ].join('\x00')
    const balanceHash = sha256(balanceSemantic)

    observations.push({
      deploymentId: config.deploymentId,
      localOwnerId: config.localOwnerId,
      sourceResourceIncarnationId: config.sourceResourceIncarnationId,
      providerVersion: `mercury:balance:${account.id}`,
      semanticHash: balanceHash,
      hashSpecificationVersion: HASH_SPEC_VERSION,
      domain: balanceDomain,
      payload: Object.freeze(balancePayload) as Readonly<Record<string, unknown>>,
      observedAt: new Date().toISOString(),
    })

    if (account.status !== 'active') continue

    let accountPage = 0
    let accountFailed = false

    while (accountPage < maxPages && !accountFailed) {
      let page: MercuryTransactionPage
      try {
        const offset = accountPage * PAGE_LIMIT
        page = await transport.fetchTransactions(account.id, offset, PAGE_LIMIT, config.since)
      } catch {
        accountFailed = true
        paginationComplete = false
        break
      }

      pagesFetched++
      const txns = page.transactions ?? []

      for (const txn of txns) {
        const status = ledgerStatus(txn.status)
        if (status === null) {
          // Cancellation/reversal and unknown lifecycle states cannot be safely
          // represented as a posted ledger fact. Quarantine the row and fail the
          // snapshot closed so its checkpoint is never promoted.
          transactionsObserved++
          quarantinedTransactions++
          paginationComplete = false
          continue
        }

        const currency = txn.currency ?? account.currency ?? 'USD'
        const domain = mercuryResourceKind(account.type)
        const effectiveDate = txn.postedAt ?? txn.createdAt
        const providerTimestamp = effectiveDate === null ? Number.NaN : Date.parse(effectiveDate)
        if (Number.isFinite(providerTimestamp)) latestProviderTimestamp = Math.max(latestProviderTimestamp ?? providerTimestamp, providerTimestamp)

        const payload = {
          transactionId: txn.id,
          accountId: txn.accountId,
          amount: decimalString(txn.amount),
          currency,
          date: effectiveDate ?? null,
          counterpartyName: txn.counterpartyName ?? null,
          bankDescription: txn.bankDescription ?? null,
          note: txn.note ?? null,
          status,
          accountName: account.name,
          accountCurrency: account.currency,
        }

        const semanticPayload = [
          config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
          domain, txn.id, decimalString(txn.amount), currency, effectiveDate ?? '', status,
        ].join('\x00')
        const semanticHash = sha256(semanticPayload)

        observations.push({
          deploymentId: config.deploymentId,
          localOwnerId: config.localOwnerId,
          sourceResourceIncarnationId: config.sourceResourceIncarnationId,
          providerVersion: `mercury:txn:${txn.id}`,
          semanticHash,
          hashSpecificationVersion: HASH_SPEC_VERSION,
          domain,
          payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
          observedAt: toIsoDate(effectiveDate),
        })

        transactionsObserved++
      }

      if (txns.length < PAGE_LIMIT) break
      if (typeof page.total === 'number' && (accountPage + 1) * PAGE_LIMIT >= page.total) break

      accountPage++
    }
    if (accountPage >= maxPages) paginationComplete = false
  }

  return {
    accounts: Object.freeze(allAccounts.filter(a => a.status === 'active')),
    observations: Object.freeze(observations),
    accountsFetched,
    pagesFetched,
    transactionsObserved,
    quarantinedTransactions,
    paginationComplete,
    nextSince: latestProviderTimestamp === null
      ? null
      : new Date(latestProviderTimestamp - CHECKPOINT_OVERLAP_MS).toISOString(),
  }
}
