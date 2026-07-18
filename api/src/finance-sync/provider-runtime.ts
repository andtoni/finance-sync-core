import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import { runPlaidSync, type PlaidTransport } from './probes/plaid/plaid-adapter'
import { runInvestecAdapter, type InvestecTransport } from './probes/investec/investec-adapter'
import { runMercurySync, type MercuryTransport } from './probes/mercury/mercury-adapter'
import { runSnapTradeSync, type SnapTradeTransport } from './probes/snaptrade/snaptrade-adapter'
import type { ProviderName } from './probes/provider-registry'
import type { RuntimeConnection, ProviderRun } from './runtime'

const PROVIDERS = new Set<ProviderName>(['investec', 'mercury', 'plaid', 'snaptrade'])
const HOSTS: Record<ProviderName, ReadonlySet<string>> = {
  investec: new Set(['openapi.investec.com']),
  mercury: new Set(['api.mercury.com']),
  plaid: new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com']),
  snaptrade: new Set(['api.snaptrade.com']),
}

export interface ProviderRuntimeEnv {
  readonly FINANCE_SYNC_CONNECTIONS_JSON?: string
  readonly FINANCE_SYNC_INVESTEC_URL?: string
  readonly FINANCE_SYNC_MERCURY_URL?: string
  readonly FINANCE_SYNC_PLAID_URL?: string
  readonly FINANCE_SYNC_PLAID_CLIENT_ID?: string
  readonly FINANCE_SYNC_PLAID_SECRET?: string
  readonly FINANCE_SYNC_SNAPTRADE_URL?: string
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
  if (!env.FINANCE_SYNC_CONNECTIONS_JSON) return []
  let parsed: unknown
  try { parsed = JSON.parse(env.FINANCE_SYNC_CONNECTIONS_JSON) } catch { throw new Error('FINANCE_SYNC_CONNECTIONS_JSON is invalid JSON') }
  if (!Array.isArray(parsed)) throw new Error('FINANCE_SYNC_CONNECTIONS_JSON must be an array')
  const ids = new Set<string>()
  return parsed.map(value => {
    const c = record(value, 'invalid Finance Sync connection')
    const provider = c.provider
    if (typeof provider !== 'string' || !PROVIDERS.has(provider as ProviderName)) throw new Error('unsupported runtime provider')
    const connectionId = string(c, 'connectionId')
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
    const accessGeneration = c.accessGeneration
    if (!Number.isSafeInteger(accessGeneration) || (accessGeneration as number) < 1) throw new Error('connection accessGeneration must be positive')
    const common = {
      provider: provider as ProviderName, connectionId, deploymentId, localOwnerId,
      sourceResourceIncarnationId: string(c, 'sourceResourceIncarnationId'), accountIds,
      wealthfolioAccountIds: Object.freeze(wealthfolioAccountIds), accessGeneration: accessGeneration as number,
    }
    switch (provider) {
      case 'plaid': return { ...common, provider, accessToken: string(c, 'accessToken'), maxPages: optionalPositive(c, 'maxPages') }
      case 'investec': return {
        ...common, provider, clientId: string(c, 'clientId'), clientSecret: string(c, 'clientSecret'), apiKey: string(c, 'apiKey'),
        eligibleProductNames: stringArray(c, 'eligibleProductNames'), fromDate: string(c, 'fromDate'), toDate: string(c, 'toDate'),
        maxPages: optionalPositive(c, 'maxPages'),
      }
      case 'mercury': return { ...common, provider, apiToken: string(c, 'apiToken'), since: optionalString(c, 'since'), maxPagesPerAccount: optionalPositive(c, 'maxPagesPerAccount') }
      case 'snaptrade': return {
        ...common, provider, clientId: string(c, 'clientId'), consumerKey: string(c, 'consumerKey'), userId: string(c, 'userId'),
        userSecret: string(c, 'userSecret'), activitiesStartDate: optionalString(c, 'activitiesStartDate'), activitiesEndDate: optionalString(c, 'activitiesEndDate'),
      }
      default: throw new Error('unsupported runtime provider')
    }
  })
}

function endpoint(raw: string | undefined, fallback: string, provider: ProviderName): URL {
  let url: URL
  try { url = new URL(raw ?? fallback) } catch { throw new Error(`${provider} endpoint must be a valid URL`) }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw new Error(`${provider} endpoint must be an HTTPS URL without credentials`)
  if (!HOSTS[provider].has(url.hostname.toLowerCase())) throw new Error(`${provider} endpoint host is not allowed`)
  return url
}
async function json(response: Response, provider: ProviderName): Promise<unknown> {
  if (!response.ok) throw new Error(`${provider} request failed: HTTP ${response.status}`)
  try { return await response.json() } catch { throw new Error(`${provider} returned invalid JSON`) }
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
      const clientId = env.FINANCE_SYNC_PLAID_CLIENT_ID, secret = env.FINANCE_SYNC_PLAID_SECRET
      if (!clientId || !secret) throw new Error('Plaid server credentials are required')
      const request = async (path: string, body: Record<string, unknown>) => json(await fetch(new URL(path, base), {
        method: 'POST', redirect: 'error', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ client_id: clientId, secret, access_token: connection.accessToken, ...body }),
      }), 'plaid')
      return {
        async syncTransactions(_token, cursor) { return await request('/transactions/sync', { cursor: cursor ?? undefined }) as Awaited<ReturnType<PlaidTransport['syncTransactions']>> },
        async getAccounts() {
          const value = record(await request('/accounts/get', {}), 'invalid Plaid accounts response')
          if (!Array.isArray(value.accounts)) throw new Error('invalid Plaid accounts response')
          return value.accounts as Awaited<ReturnType<NonNullable<PlaidTransport['getAccounts']>>>
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
          return { accounts: accounts as never[] }
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
      const request = async (path: '/holdings' | '/activities', extra: Record<string, string> = {}) => {
        const params = { clientId: connection.clientId, timestamp: String(Math.floor(Date.now() / 1000)), userId: connection.userId, userSecret: connection.userSecret, ...extra }
        const query = new URLSearchParams(Object.fromEntries(Object.entries(params).sort(([a], [b]) => a.localeCompare(b)))).toString()
        const signedPath = `/api/v1${path}`
        return json(await fetch(new URL(`${path.slice(1)}?${query}`, `${base.toString().replace(/\/+$/, '')}/`), {
          redirect: 'error', headers: { signature: await snapSign(connection.consumerKey, signedPath, query) },
        }), 'snaptrade')
      }
      return {
        async fetchAccounts() {
          const value = await request('/holdings')
          if (!Array.isArray(value)) throw new Error('invalid SnapTrade holdings response')
          return value as never[]
        },
        async fetchActivities(startDate, endDate) {
          const value = await request('/activities', { startDate, endDate })
          if (!Array.isArray(value) && !Array.isArray((value as { data?: unknown[] } | null)?.data)) throw new Error('invalid SnapTrade activities response')
          return value as Awaited<ReturnType<SnapTradeTransport['fetchActivities']>>
        },
      }
    },
  }
}

export async function runRuntimeProvider(connection: RuntimeConnection, cursor: string | null, factories: ProviderTransportFactories): Promise<ProviderRun> {
  switch (connection.provider) {
    case 'plaid': {
      const run = await runPlaidSync({ ...connection, initialCursor: cursor ?? undefined }, factories.plaid(connection))
      return { observations: run.observations, nextCursor: run.nextCursor }
    }
    case 'investec': {
      const run = await runInvestecAdapter({ ...connection }, factories.investec(connection))
      if (!run.paginationComplete) throw new Error('Investec pagination did not complete')
      return { observations: run.observations, nextCursor: null }
    }
    case 'mercury': {
      const startedAt = new Date().toISOString()
      const run = await runMercurySync({ ...connection, since: cursor ?? connection.since }, factories.mercury(connection))
      if (!run.paginationComplete) throw new Error('Mercury pagination did not complete')
      return { observations: run.observations, nextCursor: startedAt }
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
