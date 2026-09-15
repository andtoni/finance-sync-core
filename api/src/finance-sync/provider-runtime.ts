import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import { runPlaidSync, type PlaidTransport } from './probes/plaid/plaid-adapter'
import { runInvestecAdapter, type InvestecTransport } from './probes/investec/investec-adapter'
import { runMercurySync, type MercuryTransport } from './probes/mercury/mercury-adapter'
import { runSnapTradeSync, type SnapTradeTransport } from './probes/snaptrade/snaptrade-adapter'
import type { ProviderName } from './probes/provider-registry'
import type { RuntimeConnection, ProviderRun, SnapTradeAuthMode } from './runtime'
import { readExclusiveInput } from './secret-input'

const PROVIDERS = new Set<ProviderName>(['investec', 'mercury', 'plaid', 'snaptrade'])
const HOSTS: Record<ProviderName, ReadonlySet<string>> = {
  investec: new Set(['openapi.investec.com', 'openapisandbox.investec.com']),
  mercury: new Set(['api.mercury.com', 'api-sandbox.mercury.com']),
  plaid: new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com']),
  snaptrade: new Set(['api.snaptrade.com']),
}
const SNAPTRADE_ACTIVITY_PAGE_SIZE = 1000
const SNAPTRADE_MAX_ACTIVITY_ROWS = 10_000

export interface ProviderRuntimeEnv {
  readonly FINANCE_SYNC_CONNECTIONS_JSON?: string
  readonly FINANCE_SYNC_CONNECTIONS_JSON_FILE?: string
  readonly FINANCE_SYNC_PROVIDER_SECRETS_JSON?: string
  readonly FINANCE_SYNC_PROVIDER_SECRETS_JSON_FILE?: string
  readonly FINANCE_SYNC_PROVIDER_OWNER_SUBJECT?: string
  readonly FINANCE_SYNC_INVESTEC_URL?: string
  readonly FINANCE_SYNC_INVESTEC_SECRET_REFERENCE?: string
  readonly FINANCE_SYNC_MERCURY_URL?: string
  readonly FINANCE_SYNC_MERCURY_SECRET_REFERENCE?: string
  readonly FINANCE_SYNC_PLAID_URL?: string
  readonly FINANCE_SYNC_PLAID_CONNECT_SECRET_REFERENCE?: string
  readonly FINANCE_SYNC_SNAPTRADE_URL?: string
  readonly FINANCE_SYNC_SNAPTRADE_SECRET_REFERENCE?: string
}

export interface ProviderTransportFactories {
  plaid(connection: Extract<RuntimeConnection, { provider: 'plaid' }>): PlaidTransport
  investec(connection: Extract<RuntimeConnection, { provider: 'investec' }>): InvestecTransport
  mercury(connection: Extract<RuntimeConnection, { provider: 'mercury' }>): MercuryTransport
  snaptrade(connection: Extract<RuntimeConnection, { provider: 'snaptrade' }>): SnapTradeTransport
}

function record(value: unknown, message: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(message)
  return value as Record<string, unknown>
}
function string(c: Record<string, unknown>, name: string): string {
  const value = c[name]
  if (typeof value !== 'string' || value.length === 0) throw new Error(`connection ${name} is required`)
  return value
}
function optionalString(c: Record<string, unknown>, name: string): string | undefined {
  const value = c[name]
  if (value === undefined) return undefined
  if (typeof value !== 'string' || value.length === 0) throw new Error(`connection ${name} must be a non-empty string`)
  return value
}
function optionalStringRecord(c: Record<string, unknown>, name: string): Readonly<Record<string, string>> | undefined {
  const value = c[name]
  if (value === undefined) return undefined
  const recordValue = record(value, `connection ${name} must be an object`)
  const result: Record<string, string> = {}
  for (const [key, entry] of Object.entries(recordValue)) {
    if (!key || typeof entry !== 'string' || entry.length === 0) throw new Error(`connection ${name} must contain non-empty strings`)
    result[key] = entry
  }
  return Object.freeze(result)
}
function stringArray(c: Record<string, unknown>, name: string): string[] {
  const value = c[name]
  if (!Array.isArray(value) || value.length === 0 || !value.every(x => typeof x === 'string' && x.length > 0)) {
    throw new Error(`connection ${name} is required`)
  }
  if (new Set(value).size !== value.length) throw new Error(`connection ${name} must not contain duplicates`)
  return value as string[]
}
function optionalPositive(c: Record<string, unknown>, name: string): number | undefined {
  const value = c[name]
  if (value === undefined) return undefined
  if (!Number.isSafeInteger(value) || (value as number) < 1) throw new Error(`connection ${name} must be positive`)
  return value as number
}

/** Strict server-side connection parser. Unknown providers and incomplete mappings fail closed. */
export function parseRuntimeConnections(env: ProviderRuntimeEnv, deploymentId: string): readonly RuntimeConnection[] {
  const inputs = env as Readonly<Record<string, string | undefined>>
  const metadataJson = readExclusiveInput(inputs, 'FINANCE_SYNC_CONNECTIONS_JSON')
  if (!metadataJson) return []
  let parsed: unknown
  try { parsed = JSON.parse(metadataJson) } catch { throw new Error('FINANCE_SYNC_CONNECTIONS_JSON is invalid JSON') }
  if (!Array.isArray(parsed)) throw new Error('FINANCE_SYNC_CONNECTIONS_JSON must be an array')
  for (const value of parsed) {
    const metadata = record(value, 'invalid Finance Sync connection')
    for (const forbidden of ['accessToken', 'clientSecret', 'apiKey', 'apiToken', 'consumerKey', 'userSecret']) {
      if (forbidden in metadata) throw new Error('provider secret material must not appear in connection metadata')
    }
    if (typeof metadata.provider !== 'string' || !PROVIDERS.has(metadata.provider as ProviderName)) {
      throw new Error('unsupported runtime provider')
    }
  }
  let secretValue: unknown
  const secretsJson = readExclusiveInput(inputs, 'FINANCE_SYNC_PROVIDER_SECRETS_JSON', parsed.length > 0)
  try { secretValue = JSON.parse(secretsJson ?? '{}') } catch { throw new Error('FINANCE_SYNC_PROVIDER_SECRETS_JSON is invalid JSON') }
  const secrets = record(secretValue, 'FINANCE_SYNC_PROVIDER_SECRETS_JSON must be an object')
  const ids = new Set<string>()
  return parsed.map(value => {
    const metadata = record(value, 'invalid Finance Sync connection')
    for (const forbidden of ['accessToken', 'clientSecret', 'apiKey', 'apiToken', 'consumerKey', 'userSecret']) {
      if (forbidden in metadata) throw new Error('provider secret material must not appear in connection metadata')
    }
    const connectionId = string(metadata, 'connectionId')
    const secretMaterial = record(secrets[connectionId], 'provider secret material is required for every connection')
    const c = { ...metadata, ...secretMaterial }
    const provider = c.provider
    if (typeof provider !== 'string' || !PROVIDERS.has(provider as ProviderName)) throw new Error('unsupported runtime provider')
    if (ids.has(connectionId)) throw new Error('connectionId must be unique')
    ids.add(connectionId)
    const configuredDeployment = string(c, 'deploymentId')
    if (configuredDeployment !== deploymentId) throw new Error('connection deploymentId mismatch')
    const localOwnerId = assertIssuerQualifiedSubject(string(c, 'localOwnerId'))
    const accountIds = stringArray(c, 'accountIds')
    const mappings = record(c.wealthfolioAccountIds, 'connection wealthfolioAccountIds is required')
    const wealthfolioAccountIds: Record<string, string> = {}
    for (const accountId of accountIds) {
      const mapped = mappings[accountId]
      if (typeof mapped !== 'string' || mapped.length === 0) throw new Error('every provider account requires a Wealthfolio account mapping')
      wealthfolioAccountIds[accountId] = mapped
    }
    const providerGeneration = c.providerGeneration
    const receiverAccessGeneration = c.receiverAccessGeneration
    if (!Number.isSafeInteger(providerGeneration) || (providerGeneration as number) < 1) throw new Error('connection providerGeneration must be positive')
    if (!Number.isSafeInteger(receiverAccessGeneration) || (receiverAccessGeneration as number) < 1) throw new Error('connection receiverAccessGeneration must be positive')
    const common = {
      provider: provider as ProviderName, connectionId, providerConnectionId: optionalString(metadata, 'providerConnectionId'), deploymentId, localOwnerId,
      sourceResourceIncarnationId: string(c, 'sourceResourceIncarnationId'), accountIds,
      wealthfolioAccountIds: Object.freeze(wealthfolioAccountIds), providerGeneration: providerGeneration as number,
      receiverAccessGeneration: receiverAccessGeneration as number,
      secretReference: string(metadata, 'secretReference'),
    }
    switch (provider) {
      case 'plaid': return {
        ...common, provider, clientId: string(c, 'clientId'), clientSecret: string(c, 'clientSecret'),
        accessToken: string(c, 'accessToken'), sourceResourceIncarnationIds: optionalStringRecord(c, 'sourceResourceIncarnationIds'), maxPages: optionalPositive(c, 'maxPages'),
      }
      case 'investec': return {
        ...common, provider, clientId: string(c, 'clientId'), clientSecret: string(c, 'clientSecret'), apiKey: string(c, 'apiKey'),
        eligibleProductNames: stringArray(c, 'eligibleProductNames'), fromDate: string(c, 'fromDate'), toDate: string(c, 'toDate'),
        maxPages: optionalPositive(c, 'maxPages'),
      }
      case 'mercury': return { ...common, provider, apiToken: string(c, 'apiToken'), since: optionalString(c, 'since'), maxPagesPerAccount: optionalPositive(c, 'maxPagesPerAccount') }
      case 'snaptrade': {
        const rawAssetMappings = record(c.assetMappings, 'connection assetMappings is required')
        const assetMappings: Record<string, string> = {}
        for (const [providerAssetId, wealthfolioAssetId] of Object.entries(rawAssetMappings)) {
          if (!providerAssetId || typeof wealthfolioAssetId !== 'string' || !wealthfolioAssetId) throw new Error('invalid SnapTrade asset mapping')
          assetMappings[providerAssetId] = wealthfolioAssetId
        }
        const rawAuthMode = c.authMode
        const authMode: SnapTradeAuthMode = rawAuthMode === undefined || rawAuthMode === 'commercial' ? 'commercial'
          : rawAuthMode === 'personal' || rawAuthMode === 'personal-oauth' ? 'personal'
            : (() => { throw new Error('connection authMode is invalid') })()
        const userId = authMode === 'commercial' ? string(c, 'userId') : optionalString(c, 'userId')
        const userSecret = authMode === 'commercial' ? string(c, 'userSecret') : optionalString(c, 'userSecret')
        return {
          ...common, provider, clientId: string(c, 'clientId'), consumerKey: string(c, 'consumerKey'), authMode,
          ...(userId ? { userId } : {}), ...(userSecret ? { userSecret } : {}), assetMappings: Object.freeze(assetMappings),
          activitiesStartDate: optionalString(c, 'activitiesStartDate'), activitiesEndDate: optionalString(c, 'activitiesEndDate'),
        }
      }
      default: throw new Error('unsupported runtime provider')
    }
  })
}

/**
 * Plaid error codes safe to emit in diagnostics: stable, documented,
 * operator-actionable identifiers that carry no account data or provider text.
 */
const PLAID_DIAGNOSTIC_CODES = new Set([
  'TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION',
  'ITEM_LOGIN_REQUIRED',
  'INVALID_ACCESS_TOKEN',
  'INVALID_CREDENTIALS',
  'ITEM_NOT_FOUND',
  'PRODUCT_NOT_READY',
  'PRODUCT_NOT_ENABLED',
  'RATE_LIMIT_EXCEEDED',
  'INSTITUTION_DOWN',
  'INSTITUTION_NOT_RESPONDING',
  'INSTITUTION_NOT_AVAILABLE',
])

function endpoint(raw: string | undefined, fallback: string, provider: ProviderName): URL {
  let url: URL
  try { url = new URL(raw ?? fallback) } catch { throw new Error(`${provider} endpoint must be a valid URL`) }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw new Error(`${provider} endpoint must be an HTTPS URL without credentials`)
  if (!HOSTS[provider].has(url.hostname.toLowerCase())) throw new Error(`${provider} endpoint host is not allowed`)
  return url
}
async function json(response: Response, provider: ProviderName): Promise<unknown> {
  let value: unknown
  try { value = await response.json() } catch {
    if (!response.ok) throw new Error(`${provider} request failed: HTTP ${response.status}`)
    throw new Error(`${provider} returned invalid JSON`)
  }
  if (!response.ok) {
    const error = new Error(`${provider} request failed: HTTP ${response.status}`) as Error & { code?: string; status?: number }
    error.status = response.status
    // Preserve only Plaid codes from a fixed diagnostic allowlist: the
    // documented Transactions Sync restart protocol plus operator-actionable
    // item states. Arbitrary provider text is never propagated or logged.
    if (provider === 'plaid') {
      const code = (value as { error_code?: unknown } | null)?.error_code
      if (typeof code === 'string' && PLAID_DIAGNOSTIC_CODES.has(code)) error.code = code
    }
    throw error
  }
  return value
}
function canonicalJson(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (typeof value === 'object') return `{${Object.keys(value as object).sort().map(k => `${JSON.stringify(k)}:${canonicalJson((value as Record<string, unknown>)[k])}`).join(',')}}`
  return JSON.stringify(value)
}
async function snapSign(key: string, path: string, query: string): Promise<string> {
  const cryptoKey = await crypto.subtle.importKey('raw', new TextEncoder().encode(key), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const signature = await crypto.subtle.sign('HMAC', cryptoKey, new TextEncoder().encode(canonicalJson({ content: null, path, query })))
  return Buffer.from(signature).toString('base64')
}

export function createProviderTransportFactories(env: ProviderRuntimeEnv = process.env): ProviderTransportFactories {
  return {
    plaid(connection) {
      const base = endpoint(env.FINANCE_SYNC_PLAID_URL, 'https://production.plaid.com', 'plaid')
      if (base.pathname !== '/') throw new Error('Plaid endpoint must be an origin URL')
      const request = async (path: string, body: Record<string, unknown>) => json(await fetch(new URL(path, base), {
        method: 'POST', redirect: 'error', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ client_id: connection.clientId, secret: connection.clientSecret, access_token: connection.accessToken, ...body }),
      }), 'plaid')
      return {
        async syncTransactions(_token, cursor) { return await request('/transactions/sync', { cursor: cursor ?? undefined }) as Awaited<ReturnType<PlaidTransport['syncTransactions']>> },
        async getAccounts() {
          const value = record(await request('/accounts/get', {}), 'invalid Plaid accounts response')
          if (!Array.isArray(value.accounts)) throw new Error('invalid Plaid accounts response')
          return value.accounts as Awaited<ReturnType<NonNullable<PlaidTransport['getAccounts']>>>
        },
        async getLiabilities() {
          const value = record(await request('/liabilities/get', {}), 'invalid Plaid liabilities response')
          return record(value.liabilities, 'invalid Plaid liabilities response') as Awaited<ReturnType<NonNullable<PlaidTransport['getLiabilities']>>>
        },
      }
    },
    investec(connection) {
      const base = endpoint(env.FINANCE_SYNC_INVESTEC_URL, 'https://openapi.investec.com', 'investec')
      let bearer: string | undefined
      const headers = async () => {
        if (!bearer) {
          const response = await fetch(new URL('/identity/v2/oauth2/token', base), {
            method: 'POST', redirect: 'error', headers: {
              authorization: `Basic ${Buffer.from(`${connection.clientId}:${connection.clientSecret}`).toString('base64')}`,
              'x-api-key': connection.apiKey, 'content-type': 'application/x-www-form-urlencoded', 'user-agent': 'finance-sync/1.0',
            }, body: 'grant_type=client_credentials',
          })
          const token = record(await json(response, 'investec'), 'invalid Investec token response').access_token
          if (typeof token !== 'string' || !token) throw new Error('invalid Investec token response')
          bearer = token
        }
        return { authorization: `Bearer ${bearer}`, accept: 'application/json', 'user-agent': 'finance-sync/1.0' }
      }
      const get = async (url: URL) => json(await fetch(url, { redirect: 'error', headers: await headers() }), 'investec')
      return {
        async listAccounts() {
          const value = record(await get(new URL('/za/pb/v1/accounts', base)), 'invalid Investec accounts response')
          const data = record(value.data, 'invalid Investec accounts response')
          if (!Array.isArray(data.accounts)) throw new Error('invalid Investec accounts response')
          return data.accounts as Awaited<ReturnType<InvestecTransport['listAccounts']>>
        },
        async listCards() {
          const value = record(await get(new URL('/za/v1/cards', base)), 'invalid Investec cards response')
          const data = record(value.data, 'invalid Investec cards response')
          if (!Array.isArray(data.cards)) throw new Error('invalid Investec cards response')
          return data.cards.map(card => {
            const row = record(card, 'invalid Investec card response')
            return { cardKey: typeof row.CardKey === 'string' ? row.CardKey : undefined, accountId: typeof row.AccountId === 'string' ? row.AccountId : undefined, accountNumber: typeof row.AccountNumber === 'string' ? row.AccountNumber : undefined }
          })
        },
        async listCardExecutions(cardKey) {
          if (!cardKey || !/^[A-Za-z0-9_-]+$/.test(cardKey)) throw new Error('invalid Investec card key')
          const value = record(await get(new URL(`/za/v1/cards/${encodeURIComponent(cardKey)}/code/executions`, base)), 'invalid Investec card executions response')
          const data = record(value.data, 'invalid Investec card executions response')
          if (!Array.isArray(data.result)) throw new Error('invalid Investec card executions response')
          return data.result as Awaited<ReturnType<NonNullable<InvestecTransport['listCardExecutions']>>>
        },
        async getBalance(accountId) {
          const value = record(await get(new URL(`/za/pb/v1/accounts/${encodeURIComponent(accountId)}/balance`, base)), 'invalid Investec balance response')
          return record(value.data, 'invalid Investec balance response') as Awaited<ReturnType<InvestecTransport['getBalance']>>
        },
        async getTransactionPage(accountId, window, cursor) {
          const url = cursor ? new URL(cursor, base) : new URL(`/za/pb/v1/accounts/${encodeURIComponent(accountId)}/transactions`, base)
          if (url.origin !== base.origin) throw new Error('Investec pagination host is not allowed')
          if (!cursor) { url.searchParams.set('fromDate', window.fromDate); url.searchParams.set('toDate', window.toDate) }
          const value = record(await get(url), 'invalid Investec transactions response')
          const data = record(value.data, 'invalid Investec transactions response')
          if (!Array.isArray(data.transactions)) throw new Error('invalid Investec transactions response')
          const links = value.links === undefined ? {} : record(value.links, 'invalid Investec pagination response')
          const next = links.next
          if (next !== undefined && next !== null && typeof next !== 'string') throw new Error('invalid Investec pagination response')
          return { transactions: data.transactions as never[], nextCursor: next || undefined }
        },
      }
    },
    mercury(connection) {
      const base = endpoint(env.FINANCE_SYNC_MERCURY_URL, 'https://api.mercury.com/api/v1', 'mercury')
      const get = async (path: string) => json(await fetch(new URL(path.replace(/^\//, ''), `${base.toString().replace(/\/+$/, '')}/`), {
        redirect: 'error', headers: { authorization: `Bearer ${connection.apiToken}`, accept: 'application/json' },
      }), 'mercury')
      return {
        async fetchAccounts() {
          const value = await get('accounts')
          const accounts = Array.isArray(value) ? value : record(value, 'invalid Mercury accounts response').accounts
          if (!Array.isArray(accounts)) throw new Error('invalid Mercury accounts response')
          return { accounts: accounts.map(account => {
            const row = record(account, 'invalid Mercury accounts response')
            return { ...row, currency: typeof row.currency === 'string' && row.currency ? row.currency : 'USD' }
          }) as never[] }
        },
        async fetchTransactions(accountId, offset, limit, since) {
          const query = new URLSearchParams({ offset: String(offset), limit: String(limit) }); if (since) query.set('start', since)
          const value = await get(`account/${encodeURIComponent(accountId)}/transactions?${query}`)
          if (Array.isArray(value)) return { transactions: value as never[], total: value.length }
          const page = record(value, 'invalid Mercury transactions response')
          if (!Array.isArray(page.transactions) || typeof page.total !== 'number') throw new Error('invalid Mercury transactions response')
          return page as unknown as Awaited<ReturnType<MercuryTransport['fetchTransactions']>>
        },
      }
    },
    snaptrade(connection) {
      const base = endpoint(env.FINANCE_SYNC_SNAPTRADE_URL, 'https://api.snaptrade.com/api/v1', 'snaptrade')
      const request = async (path: string, extra: Record<string, string> = {}) => {
        const params: Record<string, string> = {
          clientId: connection.clientId,
          timestamp: String(Math.floor(Date.now() / 1000)),
          ...extra,
        }
        if (connection.authMode === 'commercial') {
          if (!connection.userId || !connection.userSecret) throw new Error('SnapTrade commercial credentials are incomplete')
          params.userId = connection.userId
          params.userSecret = connection.userSecret
        }
        const query = new URLSearchParams(Object.fromEntries(Object.entries(params).sort(([a], [b]) => a.localeCompare(b)))).toString()
        const signedPath = `/api/v1${path}`
        return json(await fetch(new URL(`${path.slice(1)}?${query}`, `${base.toString().replace(/\/+$/, '')}/`), {
          redirect: 'error', headers: { signature: await snapSign(connection.consumerKey, signedPath, query) },
        }), 'snaptrade')
      }
      return {
        async fetchAccounts() {
          const value = await request('/accounts')
          if (!Array.isArray(value)) throw new Error('invalid SnapTrade accounts response')
          return await Promise.all(value.map(async rawAccount => {
            const account = record(rawAccount, 'invalid SnapTrade account response')
            const accountId = string(account, 'id')
            const name = typeof account.name === 'string' && account.name ? account.name : accountId
            const balance = record(account.balance, 'invalid SnapTrade account balance response')
            const total = record(balance.total, 'invalid SnapTrade account total response')
            const totalCurrency = string(total, 'currency')
            const positionsResponse = record(await request(`/accounts/${encodeURIComponent(accountId)}/positions/all`), 'invalid SnapTrade positions response')
            const positions = positionsResponse.results
            if (!Array.isArray(positions)) throw new Error('invalid SnapTrade positions response')
            const normalizedPositions = positions.map(rawPosition => {
              const position = record(rawPosition, 'invalid SnapTrade position response')
              const instrument = record(position.instrument, 'invalid SnapTrade instrument response')
              const symbol = string(instrument, 'symbol')
              const instrumentType = typeof instrument.kind === 'string' && instrument.kind ? instrument.kind : 'other'
              const instrumentCurrency = typeof position.currency === 'string' && position.currency
                ? position.currency : typeof instrument.currency === 'string' && instrument.currency ? instrument.currency : totalCurrency
              const units = typeof position.units === 'string' || typeof position.units === 'number' ? Number(position.units) : undefined
              const price = typeof position.price === 'string' || typeof position.price === 'number' ? Number(position.price) : undefined
              const costBasis = typeof position.cost_basis === 'string' || typeof position.cost_basis === 'number' ? Number(position.cost_basis) : undefined
              if (units !== undefined && !Number.isFinite(units)) throw new Error('invalid SnapTrade position units')
              if (price !== undefined && !Number.isFinite(price)) throw new Error('invalid SnapTrade position price')
              if (costBasis !== undefined && !Number.isFinite(costBasis)) throw new Error('invalid SnapTrade position cost basis')
              return {
                symbol: {
                  id: typeof instrument.id === 'string' ? instrument.id : undefined,
                  symbol,
                  name: typeof instrument.description === 'string' ? instrument.description : symbol,
                  currency: { code: instrumentCurrency },
                  type: { code: instrumentType, description: instrumentType },
                },
                units,
                price,
                cost_basis: costBasis,
                market_value: units !== undefined && price !== undefined ? units * price : undefined,
              }
            })
            return {
              account: {
                ...account,
                id: accountId,
                name,
                ...(typeof account.number === 'string' ? { number: account.number } : {}),
              },
              total_value: { value: total.amount, currency: totalCurrency },
              positions: normalizedPositions,
            }
          })) as never[]
        },
        async fetchActivities(startDate, endDate, accountIds) {
          if (accountIds.length === 0) throw new Error('SnapTrade account-scoped activities require admitted account IDs')
          const activities = (await Promise.all(accountIds.map(async accountId => {
            const rows: Record<string, unknown>[] = []
            let offset = 0
            for (;;) {
              const value = await request(`/accounts/${encodeURIComponent(accountId)}/activities`, {
                startDate,
                endDate,
                offset: String(offset),
                limit: String(SNAPTRADE_ACTIVITY_PAGE_SIZE),
              })
              const page = Array.isArray(value) ? { data: value } : record(value, 'invalid SnapTrade activities response')
              if (!Array.isArray(page.data)) throw new Error('invalid SnapTrade activities response')
              const pageRows = page.data.map(rawActivity => record(rawActivity, 'invalid SnapTrade activity response'))
              rows.push(...pageRows)
              if (rows.length > SNAPTRADE_MAX_ACTIVITY_ROWS) throw new Error('SnapTrade activity bound exceeded')

              const pagination = page.pagination === undefined ? undefined : record(page.pagination, 'invalid SnapTrade activity pagination')
              const total = pagination?.total
              if (total !== undefined && (!Number.isSafeInteger(total) || (total as number) < 0)) throw new Error('invalid SnapTrade activity pagination')
              const nextOffset = offset + pageRows.length
              if (total !== undefined) {
                if (nextOffset >= (total as number)) break
                if (pageRows.length === 0) throw new Error('SnapTrade activity pagination ended before the reported total')
              } else if (pageRows.length === 0 || pageRows.length < SNAPTRADE_ACTIVITY_PAGE_SIZE) {
                break
              }
              if (nextOffset <= offset) throw new Error('SnapTrade activity pagination made no progress')
              offset = nextOffset
            }
            return rows.map(activity => {
              const account = activity.account
              if (account && typeof account === 'object' && !Array.isArray(account)
                && typeof (account as Record<string, unknown>).id === 'string'
                && (account as Record<string, unknown>).id) {
                if ((account as Record<string, unknown>).id !== accountId) throw new Error('SnapTrade activity account provenance mismatch')
                return activity
              }
              return {
                ...activity,
                account: {
                  ...(account && typeof account === 'object' && !Array.isArray(account) ? account : {}),
                  id: accountId,
                },
              }
            })
          }))).flat()
          return activities as Awaited<ReturnType<SnapTradeTransport['fetchActivities']>>
        },
      }
    },
  }
}

export async function runRuntimeProvider(connection: RuntimeConnection, cursor: string | null, factories: ProviderTransportFactories): Promise<ProviderRun> {
  switch (connection.provider) {
    case 'plaid': {
      const run = await runPlaidSync({ ...connection, initialCursor: cursor ?? undefined }, factories.plaid(connection))
      return {
        observations: run.observations,
        nextCursor: run.nextCursor,
        plaidSync: {
          added: run.counts.added,
          modified: run.counts.modified,
          removed: run.counts.removed,
          skippedUnadmittedAccount: run.counts.skipped.unadmittedAccount,
          skippedMissingAccountMetadata: run.counts.skipped.missingAccountMetadata,
          skippedMissingCurrency: run.counts.skipped.missingCurrency,
        },
      }
    }
    case 'investec': {
      const run = await runInvestecAdapter({
        ...connection,
        providerConnectionId: connection.connectionId,
        // The credential bundle defines the historical lower bound. The upper
        // bound must advance with every poll rather than freezing at onboarding.
        toDate: new Date().toISOString().slice(0, 10),
      }, factories.investec(connection))
      if (!run.paginationComplete) throw new Error('Investec pagination did not complete')
      return { observations: run.observations, transferAccounts: run.transferAccounts, nextCursor: null }
    }
    case 'mercury': {
      const run = await runMercurySync({ ...connection, since: cursor ?? connection.since }, factories.mercury(connection))
      if (!run.paginationComplete) throw new Error('Mercury pagination did not complete')
      // Advance only to a provider-observed timestamp with an overlap; when no
      // transaction confirms a boundary, retain the previous cursor.
      return { observations: run.observations, nextCursor: run.nextSince ?? cursor }
    }
    case 'snaptrade': {
      const end = connection.activitiesEndDate ?? new Date().toISOString().slice(0, 10)
      const run = await runSnapTradeSync({ ...connection, activitiesEndDate: end }, factories.snaptrade(connection))
      if (!run.complete) throw new Error('SnapTrade snapshot did not complete')
      return { observations: run.observations, nextCursor: end }
    }
    default: throw new Error('unsupported runtime provider')
  }
}
