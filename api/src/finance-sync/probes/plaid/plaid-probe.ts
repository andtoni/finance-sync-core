export type PlaidProbeEvidenceMode = 'synthetic' | 'sandbox' | 'constrained_live'
export type PlaidProbeOutcome = 'proven' | 'inconclusive' | 'contradicted'
export type PlaidProbeProduct = 'transactions' | 'liabilities'

export interface PlaidSyncResponse {
  readonly added?: readonly { transaction_id: string; account_id: string }[]
  readonly modified?: readonly { transaction_id: string; account_id: string }[]
  readonly removed?: readonly { transaction_id: string }[]
  readonly accounts?: readonly { account_id: string }[]
  readonly next_cursor?: string
  readonly has_more?: boolean
}

export interface PlaidProbeRequest {
  readonly endpoint: '/transactions/sync' | '/liabilities/get'
  readonly accessToken: string
  readonly cursor?: string
  readonly options: { readonly account_ids: readonly string[]; readonly days_requested: number }
}

export type PlaidProbeTransport = (request: PlaidProbeRequest) => Promise<PlaidSyncResponse>

export interface PlaidProbeConfiguration {
  readonly mode: PlaidProbeEvidenceMode
  readonly itemKey: string
  readonly accessToken: string
  readonly products: readonly PlaidProbeProduct[]
  readonly accountIds: readonly string[]
  readonly historyDays: number
  readonly initialCursor?: string
}

export interface PlaidCapabilityResult {
  readonly provider: 'plaid'
  readonly mode: PlaidProbeEvidenceMode
  readonly outcome: PlaidProbeOutcome
  readonly failureCode: 'UNSAFE_PROBE_CONFIGURATION' | 'TRANSPORT_FAILURE' | 'MUTATION_RESTART_EXHAUSTED' | null
  readonly disposable: true
  readonly projection: 'disabled'
  readonly scheduling: 'disabled'
  readonly wealthfolioWrites: 0
  readonly products: readonly PlaidProbeProduct[]
  readonly accountCount: number
  readonly historyDays: number
  readonly cursor: {
    readonly scope: 'item'
    readonly pagesStaged: number
    readonly mutationRestarts: number
    readonly promoted: boolean
  }
  readonly evidence: readonly {
    readonly kind: 'batch_counts' | 'failure'
    readonly value: string
  }[]
}

const MAX_ACCOUNTS = 4
const MAX_HISTORY_DAYS = 30
const MAX_PAGES = 10

function safe(config: PlaidProbeConfiguration): boolean {
  return config.itemKey.trim().length > 0
    && config.accessToken.trim().length > 0
    && config.accountIds.length > 0
    && config.accountIds.length <= MAX_ACCOUNTS
    && new Set(config.accountIds).size === config.accountIds.length
    && config.accountIds.every(id => id.trim().length > 0)
    && config.products.length > 0
    && config.products.length <= 2
    && new Set(config.products).size === config.products.length
    && config.products.every(product => product === 'transactions' || product === 'liabilities')
    && config.products.includes('transactions')
    && Number.isInteger(config.historyDays)
    && config.historyDays >= 1
    && config.historyDays <= MAX_HISTORY_DAYS
}

function result(
  config: PlaidProbeConfiguration,
  outcome: PlaidProbeOutcome,
  failureCode: PlaidCapabilityResult['failureCode'],
  pagesStaged: number,
  mutationRestarts: number,
  promoted: boolean,
  evidence: PlaidCapabilityResult['evidence'],
): PlaidCapabilityResult {
  return {
    provider: 'plaid', mode: config.mode, outcome, failureCode,
    disposable: true, projection: 'disabled', scheduling: 'disabled', wealthfolioWrites: 0,
    products: config.products.filter(product => product === 'transactions' || product === 'liabilities'),
    accountCount: config.accountIds.length, historyDays: config.historyDays,
    cursor: { scope: 'item', pagesStaged, mutationRestarts, promoted }, evidence,
  }
}

function mutationError(error: unknown): boolean {
  return typeof error === 'object' && error !== null
    && 'code' in error
    && error.code === 'TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION'
}

/**
 * Runs one bounded, caller-triggered probe. It owns no timer, persistence or
 * projection port: the only side effect available is the injected Plaid HTTP transport.
 */
export async function runPlaidCapabilityProbe(
  config: PlaidProbeConfiguration,
  transport: PlaidProbeTransport,
): Promise<PlaidCapabilityResult> {
  if (!safe(config)) return result(config, 'contradicted', 'UNSAFE_PROBE_CONFIGURATION', 0, 0, false, [])

  let mutationRestarts = 0
  for (;;) {
    let cursor = config.initialCursor
    let pagesStaged = 0
    let added = 0
    let modified = 0
    let removed = 0
    try {
      for (let pageNumber = 0; pageNumber < MAX_PAGES; pageNumber += 1) {
        const page = await transport({
          endpoint: '/transactions/sync', accessToken: config.accessToken, cursor,
          options: { account_ids: [...config.accountIds], days_requested: config.historyDays },
        })
        pagesStaged += 1
        added += page.added?.length ?? 0
        modified += page.modified?.length ?? 0
        removed += page.removed?.length ?? 0
        if (!page.has_more) {
          if (config.products.includes('liabilities')) {
            await transport({
              endpoint: '/liabilities/get', accessToken: config.accessToken,
              options: { account_ids: [...config.accountIds], days_requested: config.historyDays },
            })
          }
          return result(config, 'proven', null, pagesStaged, mutationRestarts, true, [{
            kind: 'batch_counts', value: `pages=${pagesStaged};added=${added};modified=${modified};removed=${removed}`,
          }])
        }
        if (!page.next_cursor) throw new Error('missing staged cursor')
        cursor = page.next_cursor
      }
      throw new Error('page bound exceeded')
    } catch (error) {
      if (mutationError(error)) {
        if (mutationRestarts === 0) {
          mutationRestarts += 1
          continue
        }
        return result(config, 'inconclusive', 'MUTATION_RESTART_EXHAUSTED', 0, mutationRestarts, false, [
          { kind: 'failure', value: 'redacted' },
        ])
      }
      return result(config, 'inconclusive', 'TRANSPORT_FAILURE', pagesStaged, mutationRestarts, false, [
        { kind: 'failure', value: 'redacted' },
      ])
    }
  }
}
