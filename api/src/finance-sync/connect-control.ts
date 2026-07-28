import { createHmac } from 'node:crypto'
import { createHash, timingSafeEqual } from 'node:crypto'
import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import type { ProviderName } from './probes/provider-registry'
import type { ProviderRuntimeEnv } from './provider-runtime'
import type { RuntimeConnection, SnapTradeRuntimeConnection } from './runtime'
import type { NativeProviderOnboarding } from './server-onboarding'
import type { AdmissionMetadata, DiscoveredProviderAsset, ProviderAssetMapping } from './onboarding-repository'

export const CONNECT_PROVIDERS = ['investec', 'mercury', 'plaid', 'snaptrade'] as const
export type ConnectProvider = (typeof CONNECT_PROVIDERS)[number]

export interface PlaidTokenWriter {
  readonly available: boolean
  /** Exact non-secret pre-provisioned destination slots; writer never derives one. */
  readonly allowedSlots: readonly string[]
  /** Authenticate and prove the durable sink is reachable before consuming a public token. */
  prepare?(): Promise<void>
  storePlaidAccessToken(input: {
    readonly subject: string
    readonly itemId: string
    readonly secretReference: string
    readonly clientId: string
    readonly clientSecret: string
    readonly accessToken: string
  }): Promise<string>
}

export interface ProviderSecretReader {
  readonly available: boolean
  /** Resolve a server-side secret reference; values are never exposed by control APIs. */
  resolve(reference: string): Promise<Readonly<Record<string, unknown>>>
}

/** @deprecated Use the separate reader and Plaid writer identities. */
export type ProviderSecretStore = PlaidTokenWriter & ProviderSecretReader

export interface PlaidConnectTransport {
  createLinkToken(subject: string): Promise<{ readonly linkToken: string }>
  exchangePublicToken(publicToken: string): Promise<{
    readonly clientId: string; readonly clientSecret: string; readonly accessToken: string; readonly itemId: string
  }>
  listAccounts(accessToken: string): Promise<readonly {
    readonly providerAccountId: string; readonly name: string; readonly type: string
    readonly currency: string; readonly mask?: string; readonly assets: readonly DiscoveredProviderAsset[]
  }[]>
}

export interface SnapTradeConnectTransport {
  createRedirectUri(connection: SnapTradeRuntimeConnection): Promise<{ readonly redirectUri: string }>
}

export interface ConnectSessionStore {
  /** Returns undefined only when another live continuation owns this exact slot. */
  create(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly secretReference: string }): Promise<string | undefined>
  consume(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string }): Promise<string | undefined>
  /** Releases the slot lease after Link bootstrap or completion success/failure. */
  release(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string }): Promise<void>
}

export interface ConnectControlDependencies {
  readonly deploymentId: string
  readonly connections: readonly RuntimeConnection[] | (() => Promise<readonly RuntimeConnection[]>)
  readonly plaid?: PlaidConnectTransport
  readonly snaptrade?: SnapTradeConnectTransport
  readonly secretStore: PlaidTokenWriter
  readonly sessionStore: ConnectSessionStore
  readonly nativeOnboarding?: NativeProviderOnboarding
  readonly onboarding?: {
    listPending(subject: string, provider: string): Promise<readonly {
      connectionId: string; providerGeneration: number; providerAccountId: string; name: string; type: string; currency: string; mask?: string
      assets: readonly DiscoveredProviderAsset[]
    }[]>
    admit(input: {
      subject: string; provider: string; connectionId: string; providerAccountId: string
      wealthfolioAccountId: string; databaseStateDigest: string; providerGeneration: number
      receiverAccessGeneration: number; baselineKind: 'empty' | 'relink'; priorProviderGeneration?: number
      assetMappings: readonly ProviderAssetMapping[]
    }): Promise<AdmissionMetadata>
    revoke(subject: string, provider: string, connectionId: string): Promise<boolean>
    reconciliationStatus(subject: string, provider: string, connectionId: string, providerAccountId: string): Promise<AdmissionMetadata | null>
  }
  readonly onPlaidConnected?: (input: {
    readonly subject: string; readonly itemId: string; readonly secretReference: string
    readonly accounts: Awaited<ReturnType<PlaidConnectTransport['listAccounts']>>
  }) => Promise<void>
}

export interface ProviderConnectStatus {
  readonly provider: ConnectProvider
  readonly status: 'available' | 'configured' | 'not_configured'
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

/** Exact Wealthfolio empty-account attestation framing (Rust usize is 64-bit on supported hosts). */
export function wealthfolioEmptyAccountAttestation(input: {
  readonly authenticatedSubject: string; readonly provider: ConnectProvider
  readonly connectionId: string; readonly providerAccountId: string; readonly wealthfolioAccountId: string
  readonly receiverAccessGeneration: number
}): string {
  const hash = createHash('sha256')
  for (const field of [
    'wealthfolio-empty-account-attestation-v1', input.authenticatedSubject, input.provider,
    input.connectionId, input.providerAccountId, input.wealthfolioAccountId,
    String(input.receiverAccessGeneration), 'activities=0;snapshots=0;positions=0;valuations=0',
  ]) {
    const value = Buffer.from(field, 'utf8')
    const length = Buffer.alloc(8); length.writeBigUInt64BE(BigInt(value.length))
    hash.update(length); hash.update(value)
  }
  return `sha256:${hash.digest('hex')}`
}

export function wealthfolioProviderRelinkAttestation(input: {
  readonly authenticatedSubject: string; readonly provider: ConnectProvider
  readonly connectionId: string; readonly providerAccountId: string; readonly wealthfolioAccountId: string
  readonly receiverAccessGeneration: number; readonly priorProviderGeneration: number
  readonly providerGeneration: number
}): string {
  const hash = createHash('sha256')
  for (const field of [
    'wealthfolio-provider-relink-attestation-v1', input.authenticatedSubject, input.provider,
    input.connectionId, input.providerAccountId, input.wealthfolioAccountId,
    String(input.receiverAccessGeneration), String(input.priorProviderGeneration),
    String(input.providerGeneration), 'existing-provider-account-and-overlays-preserved',
  ]) {
    const value = Buffer.from(field, 'utf8')
    const length = Buffer.alloc(8); length.writeBigUInt64BE(BigInt(value.length))
    hash.update(length); hash.update(value)
  }
  return `sha256:${hash.digest('hex')}`
}

function exactDigest(actual: string, expected: string): boolean {
  const left = Buffer.from(actual); const right = Buffer.from(expected)
  return left.length === right.length && timingSafeEqual(left, right)
}

export class FinanceSyncConnectControl {
  constructor(private readonly deps: ConnectControlDependencies) {}

  private async allConnections(): Promise<readonly RuntimeConnection[]> {
    return typeof this.deps.connections === 'function'
      ? await this.deps.connections() : this.deps.connections
  }

  private async subjectConnections(rawSubject: string): Promise<readonly RuntimeConnection[]> {
    const boundSubject = subject(rawSubject)
    return (await this.allConnections()).filter(connection => connection.localOwnerId === boundSubject)
  }

  async list(rawSubject: string): Promise<readonly ProviderConnectStatus[]> {
    const connections = await this.subjectConnections(rawSubject)
    return CONNECT_PROVIDERS.map(name => this.providerStatus(name, connections))
  }

  async status(rawSubject: string, rawProvider: string): Promise<ProviderConnectStatus> {
    const selected = provider(rawProvider)
    const connections = await this.subjectConnections(rawSubject)
    return this.providerStatus(selected, connections)
  }

  private providerStatus(selected: ConnectProvider, connections: readonly RuntimeConnection[]): ProviderConnectStatus {
    const configured = this.isConfigured(selected, connections)
    if (configured) return { provider: selected, status: 'configured', message: 'Provider is configured.' }
    if (selected === 'plaid' && this.deps.plaid && this.deps.secretStore.available && this.deps.onPlaidConnected) {
      return { provider: selected, status: 'available', message: 'Provider is available to connect.' }
    }
    if (selected !== 'plaid' && this.deps.nativeOnboarding?.available(selected)) {
      return { provider: selected, status: 'available', message: 'Provider is available to connect.' }
    }
    return { provider: selected, status: 'not_configured', message: 'Provider is not configured.' }
  }

  private isConfigured(selected: ConnectProvider, connections: readonly RuntimeConnection[]): boolean {
    return connections.some(connection => connection.provider === selected)
  }

  async start(rawSubject: string, rawProvider: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    const selected = provider(rawProvider)
    const allConnections = await this.allConnections()
    const connections = allConnections.filter(connection => connection.localOwnerId === boundSubject)
    if (selected !== 'plaid' && !connections.some(connection => connection.provider === selected)
      && this.deps.nativeOnboarding?.available(selected)) {
      try {
        const result = await this.deps.nativeOnboarding.discover(boundSubject, selected)
        return result.status === 'link_ready'
          ? { provider: selected, status: 'link_ready', message: 'SnapTrade authorization is ready.', redirectUrl: result.redirectUrl }
          : { provider: selected, status: 'pending', message: `Choose the ${selected} accounts to add.` }
      } catch {
        throw new ConnectControlError(502, 'provider discovery failed')
      }
    }
    if (selected === 'investec' || selected === 'mercury') {
      if (!connections.some(connection => connection.provider === selected)) throw new ConnectControlError(409, 'provider is not configured')
      return { provider: selected, status: 'configured', message: 'Provider is configured.' }
    }
    if (selected === 'plaid') {
      if (!this.deps.plaid || !this.deps.secretStore.available || !this.deps.onPlaidConnected) throw new ConnectControlError(503, 'provider is unavailable')
      // Writer slots are deployment-global. Exclude every active Plaid slot,
      // not only the authenticated subject's, to prevent cross-subject overwrite.
      const activeSlots = new Set(allConnections.filter((connection): connection is Extract<RuntimeConnection, { provider: 'plaid' }> => connection.provider === 'plaid')
        .map(connection => connection.secretReference).filter((reference): reference is string => !!reference))
      const candidates = this.deps.secretStore.allowedSlots.filter(slot => !activeSlots.has(slot))
      let continuationToken: string | undefined
      for (const secretReference of candidates) {
        continuationToken = await this.deps.sessionStore.create({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', secretReference })
        if (continuationToken) break
      }
      if (!continuationToken) throw new ConnectControlError(409, 'no unallocated Plaid credential slot is available')
      try {
        const result = await this.deps.plaid.createLinkToken(boundSubject)
        if (!result.linkToken) throw new ConnectControlError(502, 'provider returned an invalid response')
        return { provider: selected, status: 'link_ready', message: 'Plaid Link is ready.', linkToken: result.linkToken, continuationToken }
      } catch (error) {
        try { await this.deps.sessionStore.release({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken }) } catch { /* expiry is the fallback lease recovery */ }
        throw error
      }
    }
    const connection = connections.find((item): item is SnapTradeRuntimeConnection => item.provider === 'snaptrade')
    if (!connection || !this.deps.snaptrade) throw new ConnectControlError(409, 'provider is not configured')
    const result = await this.deps.snaptrade.createRedirectUri(connection)
    if (!result.redirectUri) throw new ConnectControlError(502, 'provider returned an invalid response')
    return { provider: selected, status: 'link_ready', message: 'SnapTrade redirect is ready.', redirectUrl: result.redirectUri }
  }

  async completePlaid(rawSubject: string, publicToken: string, continuationToken: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    if (!publicToken || !continuationToken) throw new ConnectControlError(400, 'completion tokens are required')
    if (!this.deps.plaid || !this.deps.secretStore.available || !this.deps.onPlaidConnected) throw new ConnectControlError(503, 'provider is unavailable')
    await this.deps.secretStore.prepare?.()
    const secretReference = await this.deps.sessionStore.consume({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken })
    if (!secretReference || !this.deps.secretStore.allowedSlots.includes(secretReference)) throw new ConnectControlError(409, 'link session is invalid or already used')
    const release = async () => { try { await this.deps.sessionStore.release({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken }) } catch { /* expiry recovers a lease if PostgreSQL is temporarily unavailable */ } }
    try {
      const exchanged = await this.deps.plaid.exchangePublicToken(publicToken)
      if (!exchanged.accessToken || !exchanged.itemId) throw new ConnectControlError(502, 'provider returned an invalid response')
      const storedReference = await this.deps.secretStore.storePlaidAccessToken({
        subject: boundSubject, itemId: exchanged.itemId, secretReference, clientId: exchanged.clientId,
        clientSecret: exchanged.clientSecret, accessToken: exchanged.accessToken,
      })
      if (storedReference !== secretReference) throw new ConnectControlError(502, 'provider secret store returned an invalid slot')
      const accounts = await this.deps.plaid.listAccounts(exchanged.accessToken)
      if (accounts.length === 0) throw new ConnectControlError(502, 'provider returned no accounts')
      await this.deps.onPlaidConnected({ subject: boundSubject, itemId: exchanged.itemId, secretReference, accounts })
      await release()
      return { provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' }
    } catch (error) {
      await release()
      throw error
    }
  }

  async pendingAccounts(rawSubject: string, rawProvider: string): Promise<readonly {
    connectionId: string; providerGeneration: number; providerAccountId: string; name: string; type: string; currency: string; mask?: string
    assets: readonly DiscoveredProviderAsset[]
  }[]> {
    const boundSubject = subject(rawSubject)
    const selected = provider(rawProvider)
    if (!this.deps.onboarding) throw new ConnectControlError(503, 'provider onboarding is unavailable')
    return this.deps.onboarding.listPending(boundSubject, selected)
  }

  async admit(rawSubject: string, rawProvider: string, input: {
    authenticatedSubject: string; connectionId: string; providerAccountId: string; wealthfolioAccountId: string
    databaseStateDigest: string; providerGeneration: number; receiverAccessGeneration: number
    baselineKind: 'empty' | 'relink'; priorProviderGeneration?: number
    assetMappings: readonly ProviderAssetMapping[]
  }): Promise<AdmissionMetadata> {
    const boundSubject = subject(rawSubject)
    const selected = provider(rawProvider)
    if (!this.deps.onboarding) throw new ConnectControlError(503, 'provider onboarding is unavailable')
    for (const value of [input.authenticatedSubject, input.connectionId, input.providerAccountId, input.wealthfolioAccountId, input.databaseStateDigest]) {
      if (!value || value.length > 512 || value.includes('\0')) throw new ConnectControlError(400, 'invalid admission request')
    }
    if (input.authenticatedSubject !== boundSubject
      || !Number.isSafeInteger(input.providerGeneration) || input.providerGeneration < 1
      || !Number.isSafeInteger(input.receiverAccessGeneration) || input.receiverAccessGeneration < 1
      || (input.baselineKind !== 'empty' && input.baselineKind !== 'relink')
      || !Array.isArray(input.assetMappings) || input.assetMappings.some(mapping => !mapping || typeof mapping.providerAssetId !== 'string'
        || typeof mapping.wealthfolioAssetId !== 'string')) {
      throw new ConnectControlError(400, 'invalid admission request')
    }
    let expected: string
    if (input.baselineKind === 'empty') {
      if (input.priorProviderGeneration !== undefined) throw new ConnectControlError(400, 'invalid admission request')
      expected = wealthfolioEmptyAccountAttestation({ ...input, provider: selected })
    } else {
      if (!Number.isSafeInteger(input.priorProviderGeneration) || input.priorProviderGeneration! < 1
        || input.priorProviderGeneration! >= input.providerGeneration) {
        throw new ConnectControlError(400, 'invalid admission request')
      }
      expected = wealthfolioProviderRelinkAttestation({
        ...input, provider: selected, priorProviderGeneration: input.priorProviderGeneration!,
      })
    }
    if (!exactDigest(input.databaseStateDigest, expected)) throw new ConnectControlError(409, 'provider account could not be admitted')
    try { return await this.deps.onboarding.admit({
      subject: boundSubject, provider: selected, connectionId: input.connectionId,
      providerAccountId: input.providerAccountId, wealthfolioAccountId: input.wealthfolioAccountId,
      databaseStateDigest: input.databaseStateDigest, providerGeneration: input.providerGeneration,
      receiverAccessGeneration: input.receiverAccessGeneration, baselineKind: input.baselineKind,
      ...(input.priorProviderGeneration === undefined ? {} : { priorProviderGeneration: input.priorProviderGeneration }),
      assetMappings: input.assetMappings,
    }) }
    catch { throw new ConnectControlError(409, 'provider account could not be admitted') }
  }

  async admissionStatus(rawSubject: string, rawProvider: string, connectionId: string, providerAccountId: string): Promise<AdmissionMetadata> {
    const boundSubject = subject(rawSubject)
    const selected = provider(rawProvider)
    if (!this.deps.onboarding || !connectionId || !providerAccountId) throw new ConnectControlError(400, 'invalid admission status request')
    const metadata = await this.deps.onboarding.reconciliationStatus(boundSubject, selected, connectionId, providerAccountId)
    if (!metadata) throw new ConnectControlError(404, 'admission not found')
    return metadata
  }

  async disconnect(rawSubject: string, rawProvider: string, connectionId: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    const selected = provider(rawProvider)
    if (!this.deps.onboarding || !connectionId || connectionId.length > 512 || connectionId.includes('\0')) throw new ConnectControlError(400, 'invalid provider connection')
    try {
      if (!await this.deps.onboarding.revoke(boundSubject, selected, connectionId)) throw new ConnectControlError(404, 'provider connection not found')
      return { provider: selected, status: 'disconnected', message: 'Provider connection was disconnected.' }
    } catch (error) {
      if (error instanceof ConnectControlError) throw error
      throw new ConnectControlError(503, 'provider disconnect failed')
    }
  }
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

export function createPlaidConnectTransport(input: { readonly env: ProviderRuntimeEnv; readonly secretReader: ProviderSecretReader; readonly secretReference: string }, fetchImpl: typeof fetch = fetch): PlaidConnectTransport | undefined {
  if (!input.secretReader.available || !/^[A-Za-z0-9_-]{1,128}$/.test(input.secretReference)) return undefined
  const env = input.env
  let credentials: Promise<{ readonly clientId: string; readonly clientSecret: string }> | undefined
  const credentialBundle = () => credentials ??= input.secretReader.resolve(input.secretReference).then(bundle => {
    const clientId = bundle.clientId; const clientSecret = bundle.clientSecret
    if (typeof clientId !== 'string' || !clientId || typeof clientSecret !== 'string' || !clientSecret) throw new Error('Plaid Connect credential bundle is invalid')
    return { clientId, clientSecret }
  })
  const base = endpoint(env.FINANCE_SYNC_PLAID_URL, 'https://production.plaid.com', new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com']), 'Plaid')
  if (base.pathname !== '/') throw new Error('Plaid endpoint must be an origin URL')
  const post = async (path: string, body: Record<string, unknown>) => {
    const { clientId, clientSecret } = await credentialBundle()
    return responseJson(await fetchImpl(new URL(path, base), { method: 'POST', redirect: 'error', headers: { 'content-type': 'application/json', 'plaid-version': '2020-09-14' }, body: JSON.stringify({ client_id: clientId, secret: clientSecret, ...body }) }))
  }
  return {
    async createLinkToken(boundSubject) {
      const value = await post('/link/token/create', {
        user: { client_user_id: boundSubject }, client_name: 'Finance Sync', products: ['transactions', 'liabilities'],
        country_codes: ['US'], language: 'en',
      })
      if (typeof value.link_token !== 'string' || !value.link_token) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { linkToken: value.link_token }
    },
    async exchangePublicToken(publicToken) {
      const value = await post('/item/public_token/exchange', { public_token: publicToken })
      if (typeof value.access_token !== 'string' || !value.access_token || typeof value.item_id !== 'string' || !value.item_id) {
        throw new ConnectControlError(502, 'provider returned an invalid response')
      }
      const { clientId, clientSecret } = await credentialBundle()
      return { clientId, clientSecret, accessToken: value.access_token, itemId: value.item_id }
    },
    async listAccounts(accessToken) {
      const value = await post('/accounts/get', { access_token: accessToken })
      if (!Array.isArray(value.accounts)) throw new ConnectControlError(502, 'provider returned an invalid response')
      return value.accounts.map(item => {
        if (!item || typeof item !== 'object' || Array.isArray(item)) throw new ConnectControlError(502, 'provider returned an invalid response')
        const account = item as Record<string, unknown>
        const balances = account.balances && typeof account.balances === 'object' && !Array.isArray(account.balances)
          ? account.balances as Record<string, unknown> : {}
        const providerAccountId = account.account_id
        const name = account.official_name ?? account.name
        const type = account.subtype ?? account.type
        const currency = balances.iso_currency_code ?? balances.unofficial_currency_code
        if (typeof providerAccountId !== 'string' || !providerAccountId || typeof name !== 'string' || !name
          || typeof type !== 'string' || !type || typeof currency !== 'string' || !currency) {
          throw new ConnectControlError(502, 'provider returned an invalid response')
        }
        if (account.mask !== undefined && account.mask !== null && typeof account.mask !== 'string') throw new ConnectControlError(502, 'provider returned an invalid response')
        return { providerAccountId, name, type, currency, assets: [], ...(typeof account.mask === 'string' ? { mask: account.mask } : {}) }
      })
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
        clientId: connection.clientId, connectionType: 'read', timestamp: String(Math.floor(Date.now() / 1000)),
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
      let redirect: URL
      try { redirect = new URL(redirectUri) } catch { throw new ConnectControlError(502, 'provider returned an invalid response') }
      if (redirect.protocol !== 'https:' || redirect.hostname.toLowerCase() !== 'app.snaptrade.com' || redirect.pathname !== '/snapTrade/redeemToken' || redirect.username || redirect.password) {
        throw new ConnectControlError(502, 'provider returned an invalid response')
      }
      return { redirectUri: redirect.toString() }
    },
  }
}

/** Production remains fail-closed until the exact Infisical writer slot policy is approved. */
export const unavailableProviderSecretStore: ProviderSecretStore = {
  available: false,
  allowedSlots: [],
  async storePlaidAccessToken() { throw new ConnectControlError(503, 'provider secret store is unavailable') },
  async resolve() { throw new ConnectControlError(503, 'provider secret store is unavailable') },
}

export function isProviderName(value: string): value is ProviderName {
  return (CONNECT_PROVIDERS as readonly string[]).includes(value)
}
