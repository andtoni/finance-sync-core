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
  readonly official_name?: string | null
  readonly mask?: string | null
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
  /** Per-account native resource identity for item-level multi-account sync. */
  readonly sourceResourceIncarnationIds?: Readonly<Record<string, string>>
  readonly accountIds: readonly string[]
  /**
   * Stored identity of admitted accounts, keyed by admitted account id. An
   * institution can re-issue an account under a new Plaid account id; a
   * returned account that is not admitted and matches exactly one candidate
   * that was not returned under its own id is ingested as that candidate.
   */
  readonly reissueCandidates?: Readonly<Record<string, PlaidReissueCandidate>>
  readonly initialCursor?: string
  readonly maxPages?: number
}

export interface PlaidReissueCandidate {
  readonly mask: string
  readonly name: string
  readonly currency: string
  readonly sourceResourceIncarnationId: string
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
  /** Database-assigned durable ordering token, populated after persistence. */
  readonly observationRevision?: number
}

export interface PlaidSyncSkippedCounts {
  readonly unadmittedAccount: number
  readonly missingAccountMetadata: number
  readonly missingCurrency: number
  /** Admitted accounts whose current-state facts were skipped: Plaid reported no usable current balance. */
  readonly missingBalance: number
  /** Admitted accounts Plaid did not return; nothing is emitted for them. */
  readonly accountNotReturned: number
}

export interface PlaidSyncCounts {
  readonly added: number
  readonly modified: number
  readonly removed: number
  readonly skipped: PlaidSyncSkippedCounts
}

export interface PlaidSyncResult {
  readonly nextCursor: string | null
  readonly accounts: readonly PlaidAccount[]
  readonly observations: readonly ObservationOutput[]
  /** Admitted accounts with no balance or liability observation in this run. */
  readonly accountsWithoutBalance: readonly string[]
  /**
   * Admitted accounts Plaid did not return. The result holds no observation
   * for them and `nextCursor` is the cursor the run started from, so a caller
   * that saves it cannot move past data that was not ingested.
   */
  readonly accountsNotReturned: readonly string[]
  /** Admitted account id to the Plaid account id it was returned under this run. */
  readonly accountsReissued: Readonly<Record<string, string>>
  readonly pagesStaged: number
  readonly mutationRestarts: number
  /** Bounded ingestion counters. Counts only — never provider text or values. */
  readonly counts: PlaidSyncCounts
}

function emptyCounts(): { added: number; modified: number; removed: number; skipped: { unadmittedAccount: number; missingAccountMetadata: number; missingCurrency: number; missingBalance: number; accountNotReturned: number } } {
  return { added: 0, modified: 0, removed: 0, skipped: { unadmittedAccount: 0, missingAccountMetadata: 0, missingCurrency: 0, missingBalance: 0, accountNotReturned: 0 } }
}

function sha256(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

const HASH_SPEC_VERSION = 2
const MAX_PAGES = 50

/** Returned Plaid account id to the admitted account it was re-issued for. */
function reissuedAccountIds(config: PlaidSyncConfig, returned: readonly PlaidAccount[]): Map<string, string> {
  const returnedIds = new Set(returned.map(account => account.account_id))
  const candidates = Object.entries(config.reissueCandidates ?? {}).filter(([accountId]) => !returnedIds.has(accountId))
  const unadmitted = returned.filter(account => !config.accountIds.includes(account.account_id))
  const same = (account: PlaidAccount, candidate: PlaidReissueCandidate): boolean =>
    typeof account.mask === 'string' && account.mask.length > 0 && account.mask === candidate.mask
    && (account.official_name ?? account.name) === candidate.name
    && account.balances.iso_currency_code === candidate.currency
  const result = new Map<string, string>()
  for (const account of unadmitted) {
    const matches = candidates.filter(([, candidate]) => same(account, candidate))
    // Ambiguity in either direction leaves the account unadmitted.
    if (matches.length !== 1) continue
    if (unadmitted.filter(other => same(other, matches[0][1])).length !== 1) continue
    result.set(account.account_id, matches[0][0])
  }
  return result
}

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
  let counts = emptyCounts()
  let accountsWithoutBalance: string[] = []
  let accountsNotReturned: string[] = []
  let reissued = new Map<string, string>()
  /** The admitted account a returned Plaid account id belongs to, if any. */
  const admitted = (plaidAccountId: string): string | undefined =>
    config.accountIds.includes(plaidAccountId) ? plaidAccountId : reissued.get(plaidAccountId)
  const sourceFor = (accountId: string): string =>
    config.sourceResourceIncarnationIds?.[accountId]
      ?? config.reissueCandidates?.[accountId]?.sourceResourceIncarnationId
      ?? config.sourceResourceIncarnationId
  const removedSourceIds = (): readonly string[] => {
    const ids = Object.values(config.sourceResourceIncarnationIds ?? {})
    return ids.length > 0 ? [...new Set(ids)] : [config.sourceResourceIncarnationId]
  }

  for (let attempt = 0; attempt < 3; attempt++) {
    let cursor = config.initialCursor
    pagesStaged = 0
    observations = []
    counts = emptyCounts()
    accountsWithoutBalance = []
    accountsNotReturned = []
    accountMap.clear()

    try {
      const returnedAccounts = await transport.getAccounts?.(config.accessToken) ?? []
      reissued = reissuedAccountIds(config, returnedAccounts)
      for (const account of returnedAccounts) {
        const accountId = admitted(account.account_id)
        if (accountId !== undefined) accountMap.set(accountId, account)
      }
      for (let pageNum = 0; pageNum < (config.maxPages ?? MAX_PAGES); pageNum++) {
        const page = await transport.syncTransactions(config.accessToken, cursor)

        pagesStaged++

        // Collect account metadata
        for (const acct of page.accounts ?? []) {
          const accountId = admitted(acct.account_id)
          if (accountId !== undefined && !accountMap.has(accountId)) accountMap.set(accountId, acct)
        }

        // Build observations from added transactions
        for (const txn of page.added) {
          const accountId = admitted(txn.account_id)
          if (accountId === undefined) { counts.skipped.unadmittedAccount++; continue }
          const acct = accountMap.get(accountId)
          const currency = txn.iso_currency_code ?? txn.unofficial_currency_code ?? acct?.balances.iso_currency_code

          // No currency is unresolvable: the fallback needs the account object,
          // so a missing account also lands here and is counted separately.
          if (!currency) {
            counts.skipped.missingCurrency++
            if (!acct) counts.skipped.missingAccountMetadata++
            continue
          }
          counts.added++

          const domain = plaidResourceKind(acct?.type ?? 'other')
          const sourceResourceIncarnationId = sourceFor(accountId)
          const payload = {
            transactionId: txn.transaction_id,
            accountId,
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
            config.deploymentId, config.localOwnerId, sourceResourceIncarnationId,
            domain, txn.transaction_id, JSON.stringify(payload), JSON.stringify(txn),
          ].join('\x00')
          const semanticHash = sha256(semanticPayload)
          const hashSpecificationVersion = HASH_SPEC_VERSION

          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId,
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
          const accountId = admitted(txn.account_id)
          if (accountId === undefined) { counts.skipped.unadmittedAccount++; continue }
          const acct = accountMap.get(accountId)
          const currency = txn.iso_currency_code ?? txn.unofficial_currency_code ?? acct?.balances.iso_currency_code
          if (!currency) {
            counts.skipped.missingCurrency++
            if (!acct) counts.skipped.missingAccountMetadata++
            continue
          }
          counts.modified++

          const domain = plaidResourceKind(acct?.type ?? 'other')
          const sourceResourceIncarnationId = sourceFor(accountId)
          const payload = {
            transactionId: txn.transaction_id,
            accountId,
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
            config.deploymentId, config.localOwnerId, sourceResourceIncarnationId,
            domain, txn.transaction_id, JSON.stringify(payload), JSON.stringify(txn), 'modified',
          ].join('\x00')
          const semanticHash = sha256(semanticPayload)

          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId,
            providerVersion: `plaid:txn:${txn.transaction_id}`,
            semanticHash,
            hashSpecificationVersion: HASH_SPEC_VERSION,
            domain,
            payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
            rawPayload: txn,
            observedAt: txn.date ? `${txn.date}T00:00:00.000Z` : new Date().toISOString(),
          })
        }

        // Removed: tombstone observations. Plaid omits account_id from removal
        // rows, so item-level runs stage one tombstone per admitted native
        // resource; persistence keeps only the resource with matching history.
        for (const rem of page.removed) {
          counts.removed++
          const payload = {
            transactionId: rem.transaction_id,
            isRemoved: true,
          }
          for (const sourceResourceIncarnationId of removedSourceIds()) {
            const semanticPayload = [
              config.deploymentId, config.localOwnerId, sourceResourceIncarnationId,
              rem.transaction_id, 'removed',
            ].join('\x00')
            const semanticHash = sha256(semanticPayload)

            observations.push({
              deploymentId: config.deploymentId,
              localOwnerId: config.localOwnerId,
              sourceResourceIncarnationId,
              providerVersion: `plaid:removed:${rem.transaction_id}`,
              semanticHash,
              hashSpecificationVersion: HASH_SPEC_VERSION,
              domain: 'cash_ledger',
              payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
              rawPayload: rem,
              observedAt: new Date().toISOString(),
            })
          }
        }

        if (!page.has_more) {
          const snapshotObservedAt = new Date().toISOString()
          const liabilityAccounts = [...accountMap.values()].filter(account => account.type === 'credit' || account.type === 'loan')
          const liabilities = liabilityAccounts.length > 0
            ? await transport.getLiabilities?.(config.accessToken) ?? {}
            : {}
          const liabilityRows = [...(liabilities.credit ?? []), ...(liabilities.loan ?? []), ...(liabilities.student ?? []), ...(liabilities.mortgage ?? [])]
          const guestAccountIds = [...reissued.values()].filter(accountId => !config.accountIds.includes(accountId))
          for (const accountId of [...config.accountIds, ...guestAccountIds]) {
            const account = accountMap.get(accountId)
            // Plaid did not return this admitted account. Its type is unknown,
            // so nothing can be classified or emitted for it; the Item's other
            // accounts proceed and the cursor is held below.
            if (!account) {
              counts.skipped.accountNotReturned++
              accountsNotReturned.push(accountId)
              continue
            }
            // Plaid can return an account with no current balance. Only that
            // account's balance and liability facts are skipped and counted; a
            // balance is never invented or carried forward, and the account's
            // transactions and every other account proceed.
            if (account.balances.current === null || !Number.isFinite(account.balances.current)) {
              counts.skipped.missingBalance++
              accountsWithoutBalance.push(accountId)
              continue
            }
            const accountCurrency = account.balances.iso_currency_code
            if (!accountCurrency) throw new Error('INCOMPLETE_ACCOUNT_BALANCE_CURRENCY')
            const sourceResourceIncarnationId = sourceFor(accountId)
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
              sourceResourceIncarnationId,
              providerVersion: `plaid:balance:${accountId}`,
              semanticHash: sha256([config.deploymentId, config.localOwnerId, sourceResourceIncarnationId, 'balance', JSON.stringify(balancePayload)].join('\x00')),
              hashSpecificationVersion: HASH_SPEC_VERSION,
              domain: plaidResourceKind(account.type), payload: balancePayload, rawPayload: account, observedAt: snapshotObservedAt,
            })
            if (account.type === 'credit' || account.type === 'loan') {
              const details = liabilityRows.filter(row => row.account_id === account.account_id)
              const liabilityPayload = Object.freeze({ accountId, currentBalance: String(Math.abs(account.balances.current)), currency: accountCurrency, details })
              observations.push({
                deploymentId: config.deploymentId, localOwnerId: config.localOwnerId,
                sourceResourceIncarnationId,
                providerVersion: `plaid:liability:${accountId}`,
                semanticHash: sha256([config.deploymentId, config.localOwnerId, sourceResourceIncarnationId, 'liability', JSON.stringify(liabilityPayload)].join('\x00')),
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
          if (accountsNotReturned.length > 0) {
            observations = observations.filter(observation => {
              const accountId = observation.payload.accountId
              if (typeof accountId !== 'string' || !accountsNotReturned.includes(accountId)) return true
              // Keep the counters equal to what is emitted.
              if (observation.payload.isModified === true) counts.modified--
              else counts.added--
              return false
            })
          }
          return {
            nextCursor: accountsNotReturned.length > 0 ? config.initialCursor ?? null : page.next_cursor,
            accounts: [...accountMap.values()],
            observations,
            accountsWithoutBalance,
            accountsNotReturned,
            accountsReissued: Object.fromEntries([...reissued].map(([returnedId, accountId]) => [accountId, returnedId])),
            pagesStaged,
            mutationRestarts,
            counts,
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
