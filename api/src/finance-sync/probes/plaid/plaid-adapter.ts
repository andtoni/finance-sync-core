import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'

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
  readonly currency?: string
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
  readonly accounts: readonly PlaidAccount[]
  readonly next_cursor: string | null
  readonly has_more: boolean
}

export interface PlaidTransport {
  syncTransactions(accessToken: string, cursor?: string): Promise<PlaidSyncPage>
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

const HASH_SPEC_VERSION = 1
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
  const observations: ObservationOutput[] = []
  let mutationRestarts = 0
  let pagesStaged = 0

  for (let attempt = 0; attempt < 3; attempt++) {
    let cursor = config.initialCursor
    pagesStaged = 0

    try {
      for (let pageNum = 0; pageNum < (config.maxPages ?? MAX_PAGES); pageNum++) {
        const page = await transport.syncTransactions(config.accessToken, cursor)

        pagesStaged++

        // Collect account metadata
        for (const acct of page.accounts) {
          if (!accountMap.has(acct.account_id)) accountMap.set(acct.account_id, acct)
        }

        // Build observations from added transactions
        for (const txn of page.added) {
          const acct = accountMap.get(txn.account_id)
          const currency = txn.currency ?? acct?.balances.iso_currency_code

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
            plaidAccountName: acct?.name ?? null,
            plaidAccountType: acct?.type ?? null,
            accountCurrency: currency,
          }
          const semanticPayload = [
            config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
            domain, txn.transaction_id, String(txn.amount), currency, txn.date,
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
            observedAt: txn.date ? `${txn.date}T00:00:00.000Z` : new Date().toISOString(),
          })
        }

        // Modified transactions: produce revision observations
        for (const txn of page.modified) {
          const acct = accountMap.get(txn.account_id)
          const currency = txn.currency ?? acct?.balances.iso_currency_code
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
            isModified: true,
          }
          const semanticPayload = [
            config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
            domain, txn.transaction_id, String(txn.amount), currency, txn.date, 'modified',
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
            observedAt: new Date().toISOString(),
          })
        }

        if (!page.has_more) {
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
