import { createHmac } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import type { ProviderName } from './probes/provider-registry'
import type { ProviderRuntimeEnv } from './provider-runtime'
import type { RuntimeConnection, SnapTradeRuntimeConnection } from './runtime'

export const CONNECT_PROVIDERS = ['investec', 'mercury', 'plaid', 'snaptrade'] as const
export type ConnectProvider = (typeof CONNECT_PROVIDERS)[number]

export interface ProviderSecretStore {
  storePlaidAccessToken(input: {
    readonly subject: string
    readonly itemId: string
    readonly accessToken: string
  }): Promise<void>
}

export interface PlaidConnectTransport {
  createLinkToken(subject: string): Promise<{ readonly linkToken: string }>
  exchangePublicToken(publicToken: string): Promise<{ readonly accessToken: string; readonly itemId: string }>
}

export interface SnapTradeConnectTransport {
  createRedirectUri(connection: SnapTradeRuntimeConnection): Promise<{ readonly redirectUri: string }>
}

export interface ConnectControlDependencies {
  readonly connections: readonly RuntimeConnection[]
  readonly plaid?: PlaidConnectTransport
  readonly snaptrade?: SnapTradeConnectTransport
  readonly secretStore: ProviderSecretStore
}

export interface ProviderConnectStatus {
  readonly provider: ConnectProvider
  readonly status: 'configured' | 'not_configured'
  readonly message: string
}

export class ConnectControlError extends Error {
  constructor(readonly status: number, message: string) { super(message) }
}

function provider(value: string): ConnectProvider {
  if (!(CONNECT_PROVIDERS as readonly string[]).includes(value)) throw new ConnectControlError(404, 'unsupported provider')
  return value as ConnectProvider
}

function subject(value: string): string {
  try { return assertIssuerQualifiedSubject(value) }
  catch { throw new ConnectControlError(400, 'invalid subject') }
}

export class FinanceSyncConnectControl {
  constructor(private readonly deps: ConnectControlDependencies) {}

  private subjectConnections(rawSubject: string): readonly RuntimeConnection[] {
    const boundSubject = subject(rawSubject)
    return this.deps.connections.filter(connection => connection.localOwnerId === boundSubject)
  }

  list(rawSubject: string): readonly ProviderConnectStatus[] {
    const connections = this.subjectConnections(rawSubject)
    return CONNECT_PROVIDERS.map(name => this.providerStatus(name, connections))
  }

  status(rawSubject: string, rawProvider: string): ProviderConnectStatus {
    const selected = provider(rawProvider)
    const connections = this.subjectConnections(rawSubject)
    return this.providerStatus(selected, connections)
  }

  private providerStatus(selected: ConnectProvider, connections: readonly RuntimeConnection[]): ProviderConnectStatus {
    const configured = this.isConfigured(selected, connections)
    return { provider: selected, status: configured ? 'configured' : 'not_configured', message: configured ? 'Provider is configured.' : 'Provider is not configured.' }
  }

  private isConfigured(selected: ConnectProvider, connections: readonly RuntimeConnection[]): boolean {
    if (selected === 'plaid') return this.deps.plaid !== undefined || connections.some(connection => connection.provider === selected)
    return connections.some(connection => connection.provider === selected)
  }

  async start(rawSubject: string, rawProvider: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    const selected = provider(rawProvider)
    const connections = this.subjectConnections(boundSubject)
    if (selected === 'investec' || selected === 'mercury') {
      if (!connections.some(connection => connection.provider === selected)) throw new ConnectControlError(409, 'provider is not configured')
      return { provider: selected, status: 'configured', message: 'Provider is configured.' }
    }
    if (selected === 'plaid') {
      if (!this.deps.plaid) throw new ConnectControlError(503, 'provider is unavailable')
      const result = await this.deps.plaid.createLinkToken(boundSubject)
      if (!result.linkToken) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { provider: selected, status: 'link_ready', message: 'Plaid Link is ready.', linkToken: result.linkToken }
    }
    const connection = connections.find((item): item is SnapTradeRuntimeConnection => item.provider === 'snaptrade')
    if (!connection || !this.deps.snaptrade) throw new ConnectControlError(409, 'provider is not configured')
    const result = await this.deps.snaptrade.createRedirectUri(connection)
    if (!result.redirectUri) throw new ConnectControlError(502, 'provider returned an invalid response')
    return { provider: selected, status: 'link_ready', message: 'SnapTrade redirect is ready.', redirectUrl: result.redirectUri }
  }

  async completePlaid(rawSubject: string, publicToken: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    if (!publicToken) throw new ConnectControlError(400, 'public token is required')
    if (!this.deps.plaid) throw new ConnectControlError(503, 'provider is unavailable')
    const exchanged = await this.deps.plaid.exchangePublicToken(publicToken)
    if (!exchanged.accessToken || !exchanged.itemId) throw new ConnectControlError(502, 'provider returned an invalid response')
    await this.deps.secretStore.storePlaidAccessToken({ subject: boundSubject, itemId: exchanged.itemId, accessToken: exchanged.accessToken })
    return { provider: 'plaid', status: 'configured', message: 'Plaid connection was stored.' }
  }

  async disconnect(rawSubject: string, rawProvider: string): Promise<never> {
    subject(rawSubject)
    provider(rawProvider)
    throw new ConnectControlError(501, 'provider revocation is not implemented')
  }
}

function readOptionalFile(env: Readonly<Record<string, string | undefined>>, name: string): string | undefined {
  const direct = env[name]
  const path = env[`${name}_FILE`]
  if (direct !== undefined && path !== undefined) throw new Error(`${name} and ${name}_FILE are mutually exclusive`)
  if (direct) return direct
  if (!path) return undefined
  try { return readFileSync(path, 'utf8').trim() || undefined } catch { throw new Error(`${name}_FILE could not be read`) }
}

function endpoint(raw: string | undefined, fallback: string, allowedHosts: ReadonlySet<string>, name: string): URL {
  let value: URL
  try { value = new URL(raw ?? fallback) } catch { throw new Error(`${name} endpoint must be a valid URL`) }
  if (value.protocol !== 'https:' || value.username || value.password || value.search || value.hash || !allowedHosts.has(value.hostname.toLowerCase())) {
    throw new Error(`${name} endpoint is not allowed`)
  }
  return value
}

async function responseJson(response: Response): Promise<Record<string, unknown>> {
  if (!response.ok) throw new ConnectControlError(502, 'provider request failed')
  let value: unknown
  try { value = await response.json() } catch { throw new ConnectControlError(502, 'provider returned invalid JSON') }
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new ConnectControlError(502, 'provider returned an invalid response')
  return value as Record<string, unknown>
}

export function createPlaidConnectTransport(env: ProviderRuntimeEnv = process.env, fetchImpl: typeof fetch = fetch): PlaidConnectTransport | undefined {
  const inputs = env as Readonly<Record<string, string | undefined>>
  const clientId = readOptionalFile(inputs, 'FINANCE_SYNC_PLAID_CLIENT_ID')
  const secret = readOptionalFile(inputs, 'FINANCE_SYNC_PLAID_SECRET')
  if (!clientId && !secret) return undefined
  if (!clientId || !secret) throw new Error('both Plaid server credentials are required for Connect')
  const base = endpoint(env.FINANCE_SYNC_PLAID_URL, 'https://production.plaid.com', new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com']), 'Plaid')
  if (base.pathname !== '/') throw new Error('Plaid endpoint must be an origin URL')
  const post = async (path: string, body: Record<string, unknown>) => responseJson(await fetchImpl(new URL(path, base), {
    method: 'POST', redirect: 'error', headers: { 'content-type': 'application/json', 'plaid-version': '2020-09-14' },
    body: JSON.stringify({ client_id: clientId, secret, ...body }),
  }))
  return {
    async createLinkToken(boundSubject) {
      const value = await post('/link/token/create', {
        user: { client_user_id: boundSubject }, client_name: 'Finance Sync', products: ['transactions'],
        required_if_supported_products: ['liabilities', 'investments'], country_codes: ['US'], language: 'en',
      })
      if (typeof value.link_token !== 'string' || !value.link_token) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { linkToken: value.link_token }
    },
    async exchangePublicToken(publicToken) {
      const value = await post('/item/public_token/exchange', { public_token: publicToken })
      if (typeof value.access_token !== 'string' || !value.access_token || typeof value.item_id !== 'string' || !value.item_id) {
        throw new ConnectControlError(502, 'provider returned an invalid response')
      }
      return { accessToken: value.access_token, itemId: value.item_id }
    },
  }
}

function canonicalJson(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (typeof value === 'object') return `{${Object.keys(value as object).sort().map(key => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`).join(',')}}`
  return JSON.stringify(value)
}

export function createSnapTradeConnectTransport(env: ProviderRuntimeEnv = process.env, fetchImpl: typeof fetch = fetch): SnapTradeConnectTransport {
  const base = endpoint(env.FINANCE_SYNC_SNAPTRADE_URL, 'https://api.snaptrade.com/api/v1', new Set(['api.snaptrade.com']), 'SnapTrade')
  return {
    async createRedirectUri(connection) {
      const params = {
        clientId: connection.clientId, timestamp: String(Math.floor(Date.now() / 1000)),
        userId: connection.userId, userSecret: connection.userSecret,
      }
      const query = new URLSearchParams(Object.fromEntries(Object.entries(params).sort(([a], [b]) => a.localeCompare(b)))).toString()
      const path = '/api/v1/snapTrade/login'
      const signature = createHmac('sha256', connection.consumerKey).update(canonicalJson({ content: {}, path, query })).digest('base64')
      const value = await responseJson(await fetchImpl(new URL(`snapTrade/login?${query}`, `${base.toString().replace(/\/+$/, '')}/`), {
        method: 'POST', redirect: 'error', headers: { 'content-type': 'application/json', signature }, body: '{}',
      }))
      const redirectUri = value.redirectURI ?? value.redirect_uri
      if (typeof redirectUri !== 'string' || !redirectUri) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { redirectUri }
    },
  }
}

/** Production remains fail-closed until the exact Infisical write scope is approved. */
export const unavailableProviderSecretStore: ProviderSecretStore = {
  async storePlaidAccessToken() { throw new ConnectControlError(503, 'provider secret store is unavailable') },
}

export function isProviderName(value: string): value is ProviderName {
  return (CONNECT_PROVIDERS as readonly string[]).includes(value)
}
