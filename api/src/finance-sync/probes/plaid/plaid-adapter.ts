import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'
import { merchantContext } from '../shared/merchant-context'

/**
 * Plaid account type → canonical resource domain mapping.
 */
export function plaidResourceKind(plaidType: string): CanonicalDomain {
  switch (plaidType) {
    case 'depository': case 'cash': return 'cash_ledger'
    case 'credit': case 'loan': return 'liability'
    case 'brokerage': case 'investment': return 'brokerage_activity'
    default: return 'cash_ledger'
  }
}

/** Minimal Plaid transaction fields used for canonical observation creation. */
export interface PlaidTransaction {
  readonly transaction_id: string
  readonly account_id: string
  readonly amount: number
  readonly date: string
  readonly name: string
  readonly merchant_name?: string
  readonly pending?: boolean
  readonly pending_transaction_id?: string | null
  readonly iso_currency_code?: string | null
  readonly unofficial_currency_code?: string | null
  readonly personal_finance_category?: {
    readonly primary?: string | null
    readonly detailed?: string | null
    readonly confidence_level?: string | null
  } | null
}

export interface PlaidAccount {
  readonly account_id: string
  readonly name: string
  readonly type: string
  readonly subtype: string | null
  readonly balances: {
    readonly current: number | null
    readonly iso_currency_code: string | null
  }
}

export interface PlaidSyncPage {
  readonly added: readonly PlaidTransaction[]
  readonly modified: readonly PlaidTransaction[]
  readonly removed: readonly { transaction_id: string }[]
  readonly accounts?: readonly PlaidAccount[]
  readonly next_cursor: string | null
  readonly has_more: boolean
}

export interface PlaidLiabilities {
  readonly credit?: readonly Readonly<Record<string, unknown>>[]
  readonly loan?: readonly Readonly<Record<string, unknown>>[]
  readonly student?: readonly Readonly<Record<string, unknown>>[]
  readonly mortgage?: readonly Readonly<Record<string, unknown>>[]
}

export interface PlaidTransport {
  syncTransactions(accessToken: string, cursor?: string): Promise<PlaidSyncPage>
  getAccounts?(accessToken: string): Promise<readonly PlaidAccount[]>
  getLiabilities?(accessToken: string): Promise<PlaidLiabilities>
}

export interface PlaidSyncConfig {
  readonly accessToken: string
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly accountIds: readonly string[]
  readonly initialCursor?: string
  readonly maxPages?: number
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
  readonly rawPayload?: unknown
  readonly observedAt: string
}

export interface PlaidSyncResult {
  readonly nextCursor: string | null
  readonly accounts: readonly PlaidAccount[]
  readonly observations: readonly ObservationOutput[]
  readonly pagesStaged: number
  readonly mutationRestarts: number
}

function sha256(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

const HASH_SPEC_VERSION = 2
const MAX_PAGES = 50

/**
 * Run one Plaid Transactions Sync session, producing canonical observations.
 * Handles MUTATION_DURING_PAGINATION restarts.
 * Returns cursor, accounts, and observations for canonical ingestion.
 */
export async function runPlaidSync(
  config: PlaidSyncConfig,
  transport: PlaidTransport,
): Promise<PlaidSyncResult> {
  const accountMap = new Map<string, PlaidAccount>()
  let observations: ObservationOutput[] = []
  let mutationRestarts = 0
  let pagesStaged = 0

  for (let attempt = 0; attempt < 3; attempt++) {
    let cursor = config.initialCursor
    pagesStaged = 0
    observations = []
    accountMap.clear()

    try {
      for (const account of await transport.getAccounts?.(config.accessToken) ?? []) {
        if (config.accountIds.includes(account.account_id)) accountMap.set(account.account_id, account)
      }
      for (let pageNum = 0; pageNum < (config.maxPages ?? MAX_PAGES); pageNum++) {
        const page = await transport.syncTransactions(config.accessToken, cursor)

        pagesStaged++

        // Collect account metadata
        for (const acct of page.accounts ?? []) {
          if (config.accountIds.includes(acct.account_id) && !accountMap.has(acct.account_id)) accountMap.set(acct.account_id, acct)
        }

        // Build observations from added transactions
        for (const txn of page.added) {
          if (!config.accountIds.includes(txn.account_id)) continue
          const acct = accountMap.get(txn.account_id)
          const currency = txn.iso_currency_code ?? txn.unofficial_currency_code ?? acct?.balances.iso_currency_code

          if (!currency) continue // quarantine: no currency

          const domain = plaidResourceKind(acct?.type ?? 'other')
          const payload = {
            transactionId: txn.transaction_id,
            accountId: txn.account_id,
            amount: String(txn.amount),
            currency,
            date: txn.date,
            name: txn.name,
            merchantName: txn.merchant_name ?? null,
            pending: txn.pending ?? false,
            pendingTransactionId: txn.pending_transaction_id ?? null,
            plaidAccountName: acct?.name ?? null,
            plaidAccountType: acct?.type ?? null,
            accountCurrency: currency,
            merchantContext: merchantContext({
              provider: 'plaid', rawDescription: txn.name, merchantDisplay: txn.merchant_name ?? txn.name,
              // Plaid's PFC is retained as provider evidence only; it is not a category assignment.
              amount: -txn.amount, providerHint: txn.personal_finance_category ? { personalFinanceCategory: txn.personal_finance_category } : null,
            }),
          }
          const semanticPayload = [
            config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
            domain, txn.transaction_id, JSON.stringify(payload), JSON.stringify(txn),
          ].join('\x00')
          const semanticHash = sha256(semanticPayload)
          const hashSpecificationVersion = HASH_SPEC_VERSION

          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId: config.sourceResourceIncarnationId,
            providerVersion: `plaid:txn:${txn.transaction_id}`,
            semanticHash,
            hashSpecificationVersion,
            domain,
            payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
            rawPayload: txn,
            observedAt: txn.date ? `${txn.date}T00:00:00.000Z` : new Date().toISOString(),
          })
        }

        // Modified transactions: produce revision observations
        for (const txn of page.modified) {
          if (!config.accountIds.includes(txn.account_id)) continue
          const acct = accountMap.get(txn.account_id)
          const currency = txn.iso_currency_code ?? txn.unofficial_currency_code ?? acct?.balances.iso_currency_code
          if (!currency) continue

          const domain = plaidResourceKind(acct?.type ?? 'other')
          const payload = {
            transactionId: txn.transaction_id,
            accountId: txn.account_id,
            amount: String(txn.amount),
            currency,
            date: txn.date,
            name: txn.name,
            merchantName: txn.merchant_name ?? null,
            pending: txn.pending ?? false,
            pendingTransactionId: txn.pending_transaction_id ?? null,
            isModified: true,
            merchantContext: merchantContext({
              provider: 'plaid', rawDescription: txn.name, merchantDisplay: txn.merchant_name ?? txn.name,
              amount: -txn.amount, providerHint: txn.personal_finance_category ? { personalFinanceCategory: txn.personal_finance_category } : null,
            }),
          }
          const semanticPayload = [
            config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
            domain, txn.transaction_id, JSON.stringify(payload), JSON.stringify(txn), 'modified',
          ].join('\x00')
          const semanticHash = sha256(semanticPayload)

          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId: config.sourceResourceIncarnationId,
            providerVersion: `plaid:txn:${txn.transaction_id}`,
            semanticHash,
            hashSpecificationVersion: HASH_SPEC_VERSION,
            domain,
            payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
            rawPayload: txn,
            observedAt: txn.date ? `${txn.date}T00:00:00.000Z` : new Date().toISOString(),
          })
        }

        // Removed: tombstone observations
        for (const rem of page.removed) {
          const payload = {
            transactionId: rem.transaction_id,
            isRemoved: true,
          }
          const semanticPayload = [
            config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
            rem.transaction_id, 'removed',
          ].join('\x00')
          const semanticHash = sha256(semanticPayload)

          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId: config.sourceResourceIncarnationId,
            providerVersion: `plaid:removed:${rem.transaction_id}`,
            semanticHash,
            hashSpecificationVersion: HASH_SPEC_VERSION,
            domain: 'cash_ledger',
            payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
            rawPayload: rem,
            observedAt: new Date().toISOString(),
          })
        }

        if (!page.has_more) {
          const snapshotObservedAt = new Date().toISOString()
          const liabilityAccounts = [...accountMap.values()].filter(account => account.type === 'credit' || account.type === 'loan')
          const liabilities = liabilityAccounts.length > 0
            ? await transport.getLiabilities?.(config.accessToken) ?? {}
            : {}
          const liabilityRows = [...(liabilities.credit ?? []), ...(liabilities.loan ?? []), ...(liabilities.student ?? []), ...(liabilities.mortgage ?? [])]
          for (const accountId of config.accountIds) {
            const account = accountMap.get(accountId)
            if (!account || account.balances.current === null || !Number.isFinite(account.balances.current)) throw new Error('INCOMPLETE_ACCOUNT_BALANCE')
            const accountCurrency = account.balances.iso_currency_code
            if (!accountCurrency) throw new Error('INCOMPLETE_ACCOUNT_BALANCE_CURRENCY')
            const balancePayload = Object.freeze({
              accountId,
              currentBalance: String(account.balances.current),
              currency: accountCurrency,
              accountType: account.type,
              accountSubtype: account.subtype,
              asOfDate: snapshotObservedAt.slice(0, 10),
            })
            observations.push({
              deploymentId: config.deploymentId, localOwnerId: config.localOwnerId,
              sourceResourceIncarnationId: config.sourceResourceIncarnationId,
              providerVersion: `plaid:balance:${accountId}`,
              semanticHash: sha256([config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'balance', JSON.stringify(balancePayload)].join('\x00')),
              hashSpecificationVersion: HASH_SPEC_VERSION,
              domain: plaidResourceKind(account.type), payload: balancePayload, rawPayload: account, observedAt: snapshotObservedAt,
            })
            if (account.type === 'credit' || account.type === 'loan') {
              const details = liabilityRows.filter(row => row.account_id === accountId)
              const liabilityPayload = Object.freeze({ accountId, currentBalance: String(Math.abs(account.balances.current)), currency: accountCurrency, details })
              observations.push({
                deploymentId: config.deploymentId, localOwnerId: config.localOwnerId,
                sourceResourceIncarnationId: config.sourceResourceIncarnationId,
                providerVersion: `plaid:liability:${accountId}`,
                semanticHash: sha256([config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'liability', JSON.stringify(liabilityPayload)].join('\x00')),
                hashSpecificationVersion: HASH_SPEC_VERSION,
                domain: 'liability', payload: liabilityPayload, rawPayload: details, observedAt: snapshotObservedAt,
              })
            }
          }

          // Plaid can return a removal before or after the posted transaction that
          // names it as its pending predecessor. Decide suppression only after the
          // complete item drain so page ordering cannot delete the successor chain.
          const supersededPendingIds = new Set(observations.flatMap(observation => {
            const value = observation.payload.pendingTransactionId
            return typeof value === 'string' && value.length > 0 ? [value] : []
          }))
          observations = observations.filter(observation => !(
            observation.payload.isRemoved === true
            && typeof observation.payload.transactionId === 'string'
            && supersededPendingIds.has(observation.payload.transactionId)
          ))
          return {
            nextCursor: page.next_cursor,
            accounts: [...accountMap.values()],
            observations,
            pagesStaged,
            mutationRestarts,
          }
        }

        cursor = page.next_cursor ?? undefined
        if (!cursor) throw new Error('MISSING_CURSOR_AFTER_HAS_MORE')
      }
      throw new Error('PAGE_BOUND_EXCEEDED')
    } catch (error: unknown) {
      if (isMutationRestartError(error) && attempt < 2) {
        mutationRestarts++
        continue // restart sync from scratch
      }
      // Re-throw non-mutation or exhausted errors
      throw error
    }
  }

  throw new Error('MUTATION_RESTART_EXHAUSTED')
}

function isMutationRestartError(error: unknown): boolean {
  return typeof error === 'object' && error !== null
    && 'code' in error
    && error.code === 'TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION'
}
