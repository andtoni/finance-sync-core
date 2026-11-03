import { createHmac } from 'node:crypto'
import { createHash, timingSafeEqual } from 'node:crypto'
import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import type { ProviderName } from './probes/provider-registry'
import type { ProviderRuntimeEnv } from './provider-runtime'
import type { PlaidReissuedAccount, PlaidRuntimeConnection, RuntimeConnection, SnapTradeRuntimeConnection } from './runtime'
import type { NativeProviderOnboarding } from './server-onboarding'
import type { AdmissionMetadata, DiscoveredProviderAsset, MovedAdmission, ProviderAssetMapping } from './onboarding-repository'

export const CONNECT_PROVIDERS = ['investec', 'mercury', 'plaid', 'snaptrade'] as const
export type ConnectProvider = (typeof CONNECT_PROVIDERS)[number]

export interface PlaidTokenWriter {
  readonly available: boolean
  /** Exact non-secret pre-provisioned destination slots; writer never derives one. */
  /** Legacy exact references remain readable and writable for existing Items. */
  readonly allowedSlots: readonly string[]
  /** Server-generated destination for a new Item; never caller supplied. */
  readonly allocatePlaidSecretReference?: (subject: string) => string
  readonly acceptsPlaidSecretReference?: (reference: string) => boolean
  /** Authenticate and reserve a generated destination before consuming a public token. */
  prepare?(reservation?: { readonly secretReference: string; readonly reservationBinding: string }): Promise<void>
  /** Read a previously persisted bundle to resume after a completion crash. */
  recoverPlaidAccessToken?: (reference: string) => Promise<Readonly<Record<string, unknown>>>
  storePlaidAccessToken(input: {
    readonly subject: string
    readonly itemId: string
    readonly secretReference: string
    readonly clientId: string
    readonly clientSecret: string
    readonly accessToken: string
    /** SHA-256 binding of the subject and DB continuation token; never the raw token. */
    readonly reservationBinding?: string
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
  createUpdateLinkToken?(subject: string, input: Pick<PlaidRuntimeConnection, 'accessToken' | 'clientId' | 'clientSecret'>): Promise<{ readonly linkToken: string }>
  /** The bank behind an Item, as Plaid names it; undefined when Plaid has no name for it. */
  institutionName?(input: Pick<PlaidRuntimeConnection, 'accessToken' | 'clientId' | 'clientSecret'>): Promise<string | undefined>
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

export type PlaidConnectFlow = 'new' | 'update'

export interface ConsumedPlaidConnectSession {
  readonly secretReference: string
  readonly recovery?: boolean
  readonly claimVersion?: number
  readonly flow: PlaidConnectFlow
  readonly providerConnectionId?: string
  readonly providerGeneration?: number
}

export interface ConnectSessionStore {
  /** Returns undefined only when another live continuation owns this exact slot. */
  create(input: {
    readonly deploymentId: string
    readonly subject: string
    readonly provider: 'plaid'
    readonly secretReference: string
    readonly flow: PlaidConnectFlow
    readonly providerConnectionId?: string
    readonly providerGeneration?: number
  }): Promise<string | undefined>
  consume(input: {
    readonly deploymentId: string
    readonly subject: string
    readonly provider: 'plaid'
    readonly token: string
    readonly flow: PlaidConnectFlow
  }): Promise<ConsumedPlaidConnectSession | undefined>
  /** Releases a bootstrap lease, or a specific completion claim; completion claims are never released by token alone. */
  release(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string; readonly claimVersion?: number }): Promise<void>
  markCredentialPersisted?(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string; readonly claimVersion: number }): Promise<void>
  markUncertain?(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string; readonly claimVersion: number }): Promise<void>
  replayCompleted?(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string; readonly flow: PlaidConnectFlow }): Promise<{ readonly itemId: string } | undefined>
  finalize?(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string; readonly claimVersion: number; readonly itemId: string }, operation: (tx: unknown) => Promise<void>): Promise<void>
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
    moveAdmission?(input: {
      subject: string; wealthfolioAccountId: string; connectionId: string; providerAccountId: string
      from?: { connectionId: string; providerAccountId: string }
    }): Promise<MovedAdmission>
    /** Names and masks of the subject's admitted accounts, used only to label a connection. */
    listConnectedAccounts?(subject: string, provider: string): Promise<readonly {
      readonly providerConnectionId: string; readonly name: string; readonly mask?: string | null
    }[]>
  }
  /**
   * Plaid Item ids whose last sync needed their owner, with the fixed reason
   * code when one is known. Owned and replaced by the sync runner each cycle;
   * a completed update clears its Item.
   */
  readonly plaidItemConditions?: Map<string, string | undefined>
  /**
   * Admissions the last sync cycle ingested under a re-issued account id, by
   * Wealthfolio account id. Owned and replaced by the sync runner each cycle;
   * a completed move clears its account.
   */
  readonly plaidReissuedAccounts?: Map<string, PlaidReissuedAccount>
  readonly onPlaidConnected?: (input: {
    readonly subject: string; readonly itemId: string; readonly secretReference: string
    readonly accounts: Awaited<ReturnType<PlaidConnectTransport['listAccounts']>>
    readonly recovery?: boolean
  }) => Promise<void>
  readonly onPlaidConnectedTransactional?: (input: {
    readonly subject: string; readonly itemId: string; readonly secretReference: string
    readonly accounts: Awaited<ReturnType<PlaidConnectTransport['listAccounts']>>; readonly recovery?: boolean
  }, tx: unknown) => Promise<void>
  readonly onPlaidAccountsUpdated?: (input: {
    readonly subject: string; readonly providerConnectionId: string; readonly secretReference: string; readonly providerGeneration: number
    readonly accounts: Awaited<ReturnType<PlaidConnectTransport['listAccounts']>>
  }) => Promise<void>
}

export interface ProviderConnectionSummary {
  /** Exact value the Plaid update route accepts as its connectionId selection. */
  readonly connectionId: string
  /** Account names and masks only; never a credential, token, or balance. */
  readonly label: string
  /** The bank's display name from Plaid; absent until Plaid has answered for this Item. */
  readonly institutionName?: string
  /** Provider ids of the Item's connected accounts, so a caller can tell which bank an account belongs to. */
  readonly providerAccountIds: readonly string[]
  /** The Item needs its owner; Link update mode with account selection is the remedy. */
  readonly needsReauth: boolean
  /** A documented Plaid ITEM_ERROR code or ACCOUNT_NOT_RETURNED; never provider text. */
  readonly reason?: string
}

export interface PlaidAdmissionSummary {
  readonly wealthfolioAccountId: string
  /** Durable connection id and provider account id the account is admitted under. */
  readonly connectionId: string
  readonly providerAccountId: string
  readonly providerGeneration: number
  /** Where the last sync cycle saw the account returned instead; the move route accepts exactly this. */
  readonly reissuedAs?: { readonly connectionId: string; readonly providerAccountId: string }
}

export interface ProviderConnectStatus {
  readonly provider: ConnectProvider
  readonly status: 'available' | 'configured' | 'not_configured'
  readonly message: string
  /** Plaid only, present when the subject has at least one connected Item. */
  readonly connections?: readonly ProviderConnectionSummary[]
}

const MAX_CONNECTION_LABEL_LENGTH = 200
const PLAID_INSTITUTION_TIMEOUT_MS = 3000

function connectionLabel(accounts: readonly { readonly name: string; readonly mask?: string | null }[]): string {
  // Provider names are display text only: control characters are dropped and
  // the length is bounded by code point so the receiver's strict check holds.
  const label = [...accounts.map(account => account.mask ? `${account.name} •••• ${account.mask}` : account.name).join(', ')
    .replace(/[\u0000-\u001f\u007f-\u009f\s]+/g, ' ').trim()]
  if (label.length === 0) return 'Plaid connection'
  return label.length > MAX_CONNECTION_LABEL_LENGTH ? `${label.slice(0, MAX_CONNECTION_LABEL_LENGTH - 1).join('')}…` : label.join('')
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

function reservationBinding(subjectValue: string, continuationToken: string): string {
  return createHash('sha256').update(`${subjectValue}\u0000${continuationToken}`, 'utf8').digest('hex')
}

export class FinanceSyncConnectControl {
  /** Bank name per Plaid Item id. An Item never changes bank, so a name is fetched once per process. */
  private readonly plaidInstitutionNames = new Map<string, string>()

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
    const plaidConnections = await this.plaidConnectionSummaries(rawSubject, connections)
    return CONNECT_PROVIDERS.map(name => name === 'plaid' && plaidConnections.length > 0
      ? { ...this.providerStatus(name, connections), connections: plaidConnections }
      : this.providerStatus(name, connections))
  }

  /** One entry per Item the update route can target, so a caller can re-authenticate exactly that Item. */
  private async plaidConnectionSummaries(rawSubject: string, connections: readonly RuntimeConnection[]): Promise<readonly ProviderConnectionSummary[]> {
    const itemIds = [...new Set(connections.flatMap(connection =>
      connection.provider === 'plaid' && connection.providerConnectionId ? [connection.providerConnectionId] : []))].sort()
    if (itemIds.length === 0) return []
    const accounts = await this.deps.onboarding?.listConnectedAccounts?.(subject(rawSubject), 'plaid') ?? []
    await Promise.all(itemIds.map(connectionId => this.loadPlaidInstitutionName(connectionId, connections)))
    return itemIds.map(connectionId => {
      const reason = this.deps.plaidItemConditions?.get(connectionId)
      const institutionName = this.plaidInstitutionNames.get(connectionId)
      return {
        connectionId,
        label: connectionLabel(accounts.filter(account => account.providerConnectionId === connectionId)),
        ...(institutionName === undefined ? {} : { institutionName }),
        providerAccountIds: [...new Set(connections.flatMap(connection =>
          connection.provider === 'plaid' && connection.providerConnectionId === connectionId ? connection.accountIds : []))].sort(),
        needsReauth: this.deps.plaidItemConditions?.has(connectionId) ?? false,
        ...(reason === undefined ? {} : { reason }),
      }
    })
  }

  /** The name is display only: a failed or slow lookup leaves it out and the next listing asks again. */
  private async loadPlaidInstitutionName(connectionId: string, connections: readonly RuntimeConnection[]): Promise<void> {
    if (this.plaidInstitutionNames.has(connectionId) || !this.deps.plaid?.institutionName) return
    const connection = connections.find((item): item is PlaidRuntimeConnection => item.provider === 'plaid' && item.providerConnectionId === connectionId)
    if (!connection) return
    try {
      const name = (await this.deps.plaid.institutionName(connection))?.replace(/[\u0000-\u001f\u007f-\u009f\s]+/g, ' ').trim()
      if (name && [...name].length <= MAX_CONNECTION_LABEL_LENGTH) this.plaidInstitutionNames.set(connectionId, name)
    } catch { /* the listing must not fail over a display name */ }
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

  private async plaidItemConnection(rawSubject: string, selectedConnectionId?: string): Promise<PlaidRuntimeConnection> {
    const items = new Map<string, PlaidRuntimeConnection>()
    for (const connection of await this.subjectConnections(rawSubject)) {
      if (connection.provider !== 'plaid' || !connection.providerConnectionId) continue
      const existing = items.get(connection.providerConnectionId)
      if (existing && (existing.secretReference !== connection.secretReference
        || existing.clientId !== connection.clientId
        || existing.clientSecret !== connection.clientSecret
        || existing.accessToken !== connection.accessToken)) {
        throw new ConnectControlError(409, 'Plaid connection credentials are inconsistent')
      }
      items.set(connection.providerConnectionId, connection)
    }
    // A re-authentication must target one exact Item: a multi-Item install cannot
    // be re-authenticated through an unqualified request, and picking one
    // implicitly could re-authorize the wrong bank connection.
    if (selectedConnectionId !== undefined) {
      const selected = items.get(selectedConnectionId)
      if (!selected) throw new ConnectControlError(404, 'provider connection not found')
      return selected
    }
    if (items.size === 0) throw new ConnectControlError(409, 'provider is not configured')
    if (items.size > 1) throw new ConnectControlError(409, 'multiple Plaid connections require selecting one')
    return [...items.values()][0]!
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
      let continuationToken: string | undefined
      if (this.deps.secretStore.allocatePlaidSecretReference) {
        // The writer generates a collision-resistant reference inside its scoped
        // namespace. PostgreSQL still binds it to this subject/session.
        const secretReference = this.deps.secretStore.allocatePlaidSecretReference(boundSubject)
        continuationToken = await this.deps.sessionStore.create({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', secretReference, flow: 'new' })
        if (continuationToken) {
          try {
            await this.deps.secretStore.prepare?.({ secretReference, reservationBinding: reservationBinding(boundSubject, continuationToken) })
          } catch (error) {
            try { await this.deps.sessionStore.release({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken }) } catch { /* expiry is the fallback lease recovery */ }
            throw error
          }
        }
      } else {
        // Compatibility for injected legacy stores and tests only.
        const activeSlots = new Set(allConnections.filter((connection): connection is Extract<RuntimeConnection, { provider: 'plaid' }> => connection.provider === 'plaid')
          .map(connection => connection.secretReference).filter((reference): reference is string => !!reference))
        for (const secretReference of this.deps.secretStore.allowedSlots.filter(slot => !activeSlots.has(slot))) {
          continuationToken = await this.deps.sessionStore.create({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', secretReference, flow: 'new' })
          if (continuationToken) break
        }
      }
      if (!continuationToken) throw new ConnectControlError(409, 'a Plaid connection is already being linked; retry after it completes')
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
    const replay = await this.deps.sessionStore.replayCompleted?.({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, flow: 'new' })
    if (replay) return { provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' }
    if (!this.deps.plaid || !this.deps.secretStore.available || !this.deps.onPlaidConnected) throw new ConnectControlError(503, 'provider is unavailable')
    if (!this.deps.secretStore.allocatePlaidSecretReference) await this.deps.secretStore.prepare?.()
    const session = await this.deps.sessionStore.consume({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, flow: 'new' })
    if (!session || session.flow !== 'new' || session.providerConnectionId !== undefined || session.providerGeneration !== undefined
      || (!this.deps.secretStore.allowedSlots.includes(session.secretReference)
        && !this.deps.secretStore.acceptsPlaidSecretReference?.(session.secretReference))) throw new ConnectControlError(409, 'link session is invalid or already used')
    const secretReference = session.secretReference
    const claimVersion = session.claimVersion
    let credentialPersisted = false
    const release = async () => { try { await this.deps.sessionStore.release({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, claimVersion }) } catch { /* expiry recovers a lease if PostgreSQL is temporarily unavailable */ } }
    const uncertain = async () => { try { if (claimVersion !== undefined) await this.deps.sessionStore.markUncertain?.({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, claimVersion }) } catch { /* the lease remains recoverable when PostgreSQL is unavailable */ } }
    const persisted = async () => { if (claimVersion !== undefined) await this.deps.sessionStore.markCredentialPersisted?.({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, claimVersion }) }
    if (session.recovery) {
      if (!this.deps.secretStore.recoverPlaidAccessToken) { await uncertain(); throw new ConnectControlError(409, 'Plaid completion is uncertain; reconnect is required') }
      try {
        const bundle = await this.deps.secretStore.recoverPlaidAccessToken(secretReference)
        const itemId = typeof bundle.itemId === 'string' ? bundle.itemId : ''
        const accessToken = typeof bundle.accessToken === 'string' ? bundle.accessToken : ''
        if (!itemId || !accessToken) throw new Error('incomplete persisted Plaid bundle')
        const accounts = await this.deps.plaid.listAccounts(accessToken)
        if (accounts.length === 0) throw new Error('provider returned no accounts')
        const complete = this.deps.onPlaidConnectedTransactional && this.deps.sessionStore.finalize
        if (complete) await this.deps.sessionStore.finalize!({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, claimVersion: claimVersion!, itemId }, tx => this.deps.onPlaidConnectedTransactional!({ subject: boundSubject, itemId, secretReference, accounts, recovery: true }, tx))
        else { await this.deps.onPlaidConnected({ subject: boundSubject, itemId, secretReference, accounts, recovery: true }); await release() }
        return { provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' }
      } catch { await uncertain(); throw new ConnectControlError(409, 'Plaid completion is uncertain; reconnect is required') }
    }
    try {
      const exchanged = await this.deps.plaid.exchangePublicToken(publicToken)
      if (!exchanged.accessToken || !exchanged.itemId) throw new ConnectControlError(502, 'provider returned an invalid response')
      let storedReference: string
      try {
        storedReference = await this.deps.secretStore.storePlaidAccessToken({
          subject: boundSubject, itemId: exchanged.itemId, secretReference, clientId: exchanged.clientId,
          clientSecret: exchanged.clientSecret, accessToken: exchanged.accessToken,
          reservationBinding: reservationBinding(boundSubject, continuationToken),
        })
      } catch {
        await uncertain()
        throw new ConnectControlError(409, 'Plaid credential persistence is uncertain; reconnect is required')
      }
      if (storedReference !== secretReference) { await uncertain(); throw new ConnectControlError(502, 'provider secret store returned an invalid slot') }
      try { await persisted(); credentialPersisted = true } catch { await uncertain(); throw new ConnectControlError(409, 'Plaid credential persistence is uncertain; reconnect is required') }
      const accounts = await this.deps.plaid.listAccounts(exchanged.accessToken)
      if (accounts.length === 0) throw new ConnectControlError(502, 'provider returned no accounts')
      const complete = this.deps.onPlaidConnectedTransactional && this.deps.sessionStore.finalize
      if (complete) await this.deps.sessionStore.finalize!({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, claimVersion: claimVersion!, itemId: exchanged.itemId }, tx => this.deps.onPlaidConnectedTransactional!({ subject: boundSubject, itemId: exchanged.itemId, secretReference, accounts }, tx))
      else { await this.deps.onPlaidConnected({ subject: boundSubject, itemId: exchanged.itemId, secretReference, accounts }); await release() }
      return { provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' }
    } catch (error) {
      if (credentialPersisted || (claimVersion !== undefined && session.recovery)
        || (error instanceof ConnectControlError && error.status === 409 && error.message.includes('persistence'))) await uncertain()
      else await release()
      throw error
    }
  }

  async startPlaidUpdate(rawSubject: string, selectedConnectionId?: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    const plaid = this.deps.plaid
    if (!plaid?.createUpdateLinkToken) throw new ConnectControlError(503, 'provider update is unavailable')
    const connection = await this.plaidItemConnection(boundSubject, selectedConnectionId)
    const providerConnectionId = connection.providerConnectionId
    if (!providerConnectionId) throw new ConnectControlError(409, 'provider connection identity is unavailable')
    const continuationToken = await this.deps.sessionStore.create({
      deploymentId: this.deps.deploymentId,
      subject: boundSubject,
      provider: 'plaid',
      secretReference: connection.secretReference,
      providerConnectionId,
      providerGeneration: connection.providerGeneration,
      flow: 'update',
    })
    if (!continuationToken) throw new ConnectControlError(409, 'a Plaid account update is already in progress')
    const release = async () => { try { await this.deps.sessionStore.release({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken }) } catch { /* expiry is the fallback lease recovery */ } }
    try {
      const result = await plaid.createUpdateLinkToken(boundSubject, connection)
      if (!result.linkToken) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { provider: 'plaid', status: 'link_ready', message: 'Plaid Link is ready to add accounts.', linkToken: result.linkToken, continuationToken }
    } catch (error) {
      await release()
      throw error
    }
  }

  async completePlaidUpdate(rawSubject: string, continuationToken: string): Promise<Record<string, string>> {
    const boundSubject = subject(rawSubject)
    if (!continuationToken || !this.deps.plaid || !this.deps.onPlaidAccountsUpdated) throw new ConnectControlError(503, 'provider update is unavailable')
    const session = await this.deps.sessionStore.consume({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, flow: 'update' })
    if (!session || session.flow !== 'update' || !session.providerConnectionId || session.providerGeneration === undefined) {
      throw new ConnectControlError(409, 'link session is invalid or already used')
    }
    const release = async () => { try { await this.deps.sessionStore.release({ deploymentId: this.deps.deploymentId, subject: boundSubject, provider: 'plaid', token: continuationToken, claimVersion: session.claimVersion }) } catch { /* expiry recovers the lease if PostgreSQL is temporarily unavailable */ } }
    try {
      // The consumed session already binds the exact Item, so the completion
      // must resolve that Item rather than re-guess among the subject's Items.
      const connection = await this.plaidItemConnection(boundSubject, session.providerConnectionId)
      if (connection.providerConnectionId !== session.providerConnectionId || connection.secretReference !== session.secretReference
        || connection.providerGeneration !== session.providerGeneration) {
        throw new ConnectControlError(409, 'Plaid connection changed during the update')
      }
      const accounts = await this.deps.plaid.listAccounts(connection.accessToken)
      if (accounts.length === 0) throw new ConnectControlError(502, 'provider returned no accounts')
      await this.deps.onPlaidAccountsUpdated({
        subject: boundSubject,
        providerConnectionId: session.providerConnectionId,
        secretReference: session.secretReference,
        providerGeneration: session.providerGeneration,
        accounts,
      })
      await release()
      // The Item just answered with its accounts under the same access token;
      // the next sync confirms whether its condition is resolved.
      this.deps.plaidItemConditions?.delete(session.providerConnectionId)
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

  /**
   * The subject's connected Plaid accounts and where each is admitted. An
   * account the last sync cycle ingested under a re-issued account id names
   * that account too, so its admission can be moved there.
   */
  async plaidAdmissions(rawSubject: string): Promise<readonly PlaidAdmissionSummary[]> {
    return (await this.subjectConnections(rawSubject)).flatMap(connection => {
      const [providerAccountId] = connection.accountIds
      const suffix = `:${providerAccountId}`
      if (connection.provider !== 'plaid' || connection.accountIds.length !== 1 || !connection.connectionId.endsWith(suffix)) return []
      const wealthfolioAccountId = connection.wealthfolioAccountIds[providerAccountId!]
      if (!wealthfolioAccountId) return []
      const connectionId = connection.connectionId.slice(0, -suffix.length)
      const reissued = this.deps.plaidReissuedAccounts?.get(wealthfolioAccountId)
      return [{
        wealthfolioAccountId, connectionId, providerAccountId: providerAccountId!, providerGeneration: connection.providerGeneration,
        ...(reissued?.from.connectionId === connectionId && reissued.from.providerAccountId === providerAccountId ? { reissuedAs: reissued.to } : {}),
      }]
    }).sort((a, b) => a.wealthfolioAccountId.localeCompare(b.wealthfolioAccountId))
  }

  /**
   * Makes permanent what the sync cycle already does for a re-issued account:
   * the admission moves to the connection and account id it is returned under.
   * Only a move the last cycle observed is accepted, so an account whose
   * identity matched more than one candidate can never be moved.
   */
  async moveAdmission(rawSubject: string, input: {
    wealthfolioAccountId: string; connectionId: string; providerAccountId: string
  }): Promise<MovedAdmission> {
    const boundSubject = subject(rawSubject)
    if (!this.deps.onboarding?.moveAdmission) throw new ConnectControlError(503, 'provider onboarding is unavailable')
    for (const value of [input.wealthfolioAccountId, input.connectionId, input.providerAccountId]) {
      if (!value || value.length > 512 || value.includes('\0')) throw new ConnectControlError(400, 'invalid admission move request')
    }
    const observed = this.deps.plaidReissuedAccounts?.get(input.wealthfolioAccountId)
    const from = observed?.to.connectionId === input.connectionId && observed.to.providerAccountId === input.providerAccountId
      ? observed.from : undefined
    try {
      const moved = await this.deps.onboarding.moveAdmission({ subject: boundSubject, ...input, ...(from === undefined ? {} : { from }) })
      this.deps.plaidReissuedAccounts?.delete(input.wealthfolioAccountId)
      return moved
    } catch { throw new ConnectControlError(409, 'provider admission could not be moved') }
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
  const post = async (path: string, body: Record<string, unknown>, suppliedCredentials?: { readonly clientId: string; readonly clientSecret: string }, signal?: AbortSignal) => {
    const { clientId, clientSecret } = suppliedCredentials ?? await credentialBundle()
    return responseJson(await fetchImpl(new URL(path, base), { method: 'POST', redirect: 'error', signal, headers: { 'content-type': 'application/json', 'plaid-version': '2020-09-14' }, body: JSON.stringify({ client_id: clientId, secret: clientSecret, ...body }) }))
  }
  return {
    async createLinkToken(boundSubject) {
      const value = await post('/link/token/create', {
        user: { client_user_id: boundSubject }, client_name: 'Finance Sync', products: ['transactions'],
        additional_consented_products: ['liabilities', 'investments'],
        country_codes: ['US'], language: 'en',
      })
      if (typeof value.link_token !== 'string' || !value.link_token) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { linkToken: value.link_token }
    },
    async createUpdateLinkToken(boundSubject, input) {
      if (!input.accessToken) throw new ConnectControlError(502, 'provider credential is unavailable')
      const value = await post('/link/token/create', {
        user: { client_user_id: boundSubject }, client_name: 'Finance Sync',
        access_token: input.accessToken,
        update: { account_selection_enabled: true },
        country_codes: ['US'], language: 'en',
      }, input)
      if (typeof value.link_token !== 'string' || !value.link_token) throw new ConnectControlError(502, 'provider returned an invalid response')
      return { linkToken: value.link_token }
    },
    async institutionName(input) {
      // Bounded so a slow Plaid cannot hold up the connection listing this feeds.
      const value = await post('/item/get', { access_token: input.accessToken }, input, AbortSignal.timeout(PLAID_INSTITUTION_TIMEOUT_MS))
      const item = value.item && typeof value.item === 'object' && !Array.isArray(value.item) ? value.item as Record<string, unknown> : {}
      return typeof item.institution_name === 'string' && item.institution_name ? item.institution_name : undefined
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
      const params: Record<string, string> = {
        clientId: connection.clientId, connectionType: 'read', timestamp: String(Math.floor(Date.now() / 1000)),
      }
      if (connection.authMode === 'commercial') {
        if (!connection.userId || !connection.userSecret) throw new ConnectControlError(502, 'SnapTrade commercial credentials are incomplete')
        params.userId = connection.userId
        params.userSecret = connection.userSecret
      }
      const query = new URLSearchParams(Object.fromEntries(Object.entries(params).sort(([a], [b]) => a.localeCompare(b)))).toString()
      const path = '/api/v1/snapTrade/login'
      // SnapTrade treats an empty login request as a null body. Signing and
      // sending `{}` produces a misleading 401 signature error, especially
      // for Personal API keys.
      const signature = createHmac('sha256', connection.consumerKey).update(canonicalJson({ content: null, path, query })).digest('base64')
      const value = await responseJson(await fetchImpl(new URL(`snapTrade/login?${query}`, `${base.toString().replace(/\/+$/, '')}/`), {
        method: 'POST', redirect: 'error', headers: { signature },
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
