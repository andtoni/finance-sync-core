import type { ProviderSecretReader, SnapTradeConnectTransport } from './connect-control'
import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import type { DiscoveredAccount, PostgresOnboardingRepository } from './onboarding-repository'
import { discoverInvestecAccounts } from './probes/investec/investec-adapter'
import type { ProviderRuntimeEnv, ProviderTransportFactories } from './provider-runtime'
import type { InvestecRuntimeConnection, MercuryRuntimeConnection, SnapTradeRuntimeConnection } from './runtime'

export type NativeOnboardingProvider = 'investec' | 'mercury' | 'snaptrade'
export type NativeOnboardingResult =
  | { readonly status: 'pending' }
  | { readonly status: 'link_ready'; readonly redirectUrl: string }

export interface NativeProviderOnboarding {
  available(provider: NativeOnboardingProvider): boolean
  discover(subject: string, provider: NativeOnboardingProvider): Promise<NativeOnboardingResult>
}

const REFERENCE_ENV = {
  investec: 'FINANCE_SYNC_INVESTEC_SECRET_REFERENCE',
  mercury: 'FINANCE_SYNC_MERCURY_SECRET_REFERENCE',
  snaptrade: 'FINANCE_SYNC_SNAPTRADE_SECRET_REFERENCE',
} as const

function requiredString(bundle: Readonly<Record<string, unknown>>, name: string): string {
  const value = bundle[name]
  if (typeof value !== 'string' || !value) throw new Error(`native onboarding bundle ${name} is required`)
  return value
}
function optionalString(bundle: Readonly<Record<string, unknown>>, name: string): string | undefined {
  const value = bundle[name]
  if (value === undefined) return undefined
  if (typeof value !== 'string' || !value) throw new Error(`native onboarding bundle ${name} is invalid`)
  return value
}
function optionalPositive(bundle: Readonly<Record<string, unknown>>, name: string): number | undefined {
  const value = bundle[name]
  if (value === undefined) return undefined
  if (!Number.isSafeInteger(value) || (value as number) < 1) throw new Error(`native onboarding bundle ${name} is invalid`)
  return value as number
}
function strings(bundle: Readonly<Record<string, unknown>>, name: string): readonly string[] {
  const value = bundle[name]
  if (!Array.isArray(value) || value.length === 0 || !value.every(item => typeof item === 'string' && item.length > 0)) {
    throw new Error(`native onboarding bundle ${name} is required`)
  }
  return value as string[]
}
function common(provider: NativeOnboardingProvider, subject: string) {
  return {
    provider, connectionId: 'onboarding-discovery', deploymentId: 'onboarding-discovery', localOwnerId: subject,
    sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000000', accountIds: [],
    wealthfolioAccountIds: {}, providerGeneration: 1, receiverAccessGeneration: 1,
  }
}
function currency(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Za-z]{3}$/.test(value.trim())) throw new Error('invalid provider account currency')
  return value.trim().toUpperCase()
}
function mercuryAccounts(value: Awaited<ReturnType<ReturnType<ProviderTransportFactories['mercury']>['fetchAccounts']>>): readonly DiscoveredAccount[] {
  return Object.freeze(value.accounts.filter(account => account.status === 'active').map(account => {
    if (typeof account.id !== 'string' || !account.id || typeof account.name !== 'string' || !account.name
      || typeof account.type !== 'string' || !account.type) throw new Error('invalid Mercury account discovery response')
    return { providerAccountId: account.id, name: account.name, type: account.type, currency: currency(account.currency), assets: [] }
  }))
}
function snapTradeAccounts(value: Awaited<ReturnType<ReturnType<ProviderTransportFactories['snaptrade']>['fetchAccounts']>>): readonly DiscoveredAccount[] {
  return Object.freeze(value.map(item => {
    const account = item.account
    const id = account?.id
    const name = account?.name
    const rawCurrency = item.total_value?.currency
    if (typeof id !== 'string' || !id || typeof name !== 'string' || !name) throw new Error('invalid SnapTrade account discovery response')
    const type = account.raw_type ?? account.meta?.type ?? 'brokerage'
    const mask = account.number?.slice(-4)
    const assets = (item.positions ?? []).flatMap(position => {
      const symbol = position.symbol
      const ticker = typeof symbol?.symbol === 'string' ? symbol.symbol : symbol?.symbol?.symbol ?? symbol?.ticker
      const typeText = `${symbol?.type?.code ?? ''} ${symbol?.type?.description ?? ''}`.toLowerCase()
      if (typeText.includes('cash')) return []
      if (!ticker) throw new Error('SnapTrade holding has incomplete asset metadata')
      const providerAssetId = symbol?.id ?? (typeof symbol?.symbol === 'object' ? symbol.symbol.id : undefined)
        ?? symbol?.universal_symbol_id ?? symbol?.universal_symbol?.id
      if (typeof providerAssetId !== 'string' || !providerAssetId) throw new Error('SnapTrade holding has no stable provider asset id')
      return [{ providerAssetId, symbol: ticker, name: symbol?.name ?? ticker,
        currency: currency(symbol?.currency?.code ?? rawCurrency), instrumentType: symbol?.type?.code ?? symbol?.type?.description ?? 'unknown' }]
    })
    if (new Set(assets.map(asset => asset.providerAssetId)).size !== assets.length) throw new Error('duplicate SnapTrade provider asset id')
    return { providerAccountId: id, name, type, currency: currency(rawCurrency), ...(mask ? { mask } : {}), assets }
  }))
}

/** Server-only discovery using named Infisical bundles. No secret or provider identity enters from HTTP. */
export function createNativeProviderOnboarding(input: {
  readonly env: ProviderRuntimeEnv
  readonly secretStore: ProviderSecretReader
  readonly factories: ProviderTransportFactories
  readonly repository: Pick<PostgresOnboardingRepository, 'recordConnection'>
  readonly snaptrade: SnapTradeConnectTransport
}): NativeProviderOnboarding {
  const configuredOwner = input.env.FINANCE_SYNC_PROVIDER_OWNER_SUBJECT
  const owner = configuredOwner === undefined ? undefined : assertIssuerQualifiedSubject(configuredOwner)
  const references = {} as Partial<Record<NativeOnboardingProvider, string>>
  for (const provider of Object.keys(REFERENCE_ENV) as NativeOnboardingProvider[]) {
    const reference = input.env[REFERENCE_ENV[provider]]
    if (reference !== undefined) {
      if (!/^[A-Z0-9_]{1,256}$/.test(reference)) throw new Error(`${REFERENCE_ENV[provider]} is invalid`)
      references[provider] = reference
    }
  }
  return {
    available: provider => input.secretStore.available && owner !== undefined && references[provider] !== undefined,
    async discover(subject, provider) {
      // Single-owner v1: reject before resolving a secret or making any provider call.
      if (!owner || subject !== owner) throw new Error('native provider owner is not authorized')
      const secretReference = references[provider]
      if (!input.secretStore.available || !secretReference) throw new Error('native provider is unavailable')
      const bundle = await input.secretStore.resolve(secretReference)
      const providerConnectionId = requiredString(bundle, 'providerConnectionId')
      let accounts: readonly DiscoveredAccount[]
      let configuration: Readonly<Record<string, unknown>> = {}
      if (provider === 'investec') {
        const eligibleProductNames = strings(bundle, 'eligibleProductNames')
        const connection: InvestecRuntimeConnection = {
          ...common(provider, subject), provider: 'investec', clientId: requiredString(bundle, 'clientId'), clientSecret: requiredString(bundle, 'clientSecret'),
          apiKey: requiredString(bundle, 'apiKey'), eligibleProductNames, fromDate: requiredString(bundle, 'fromDate'),
          toDate: requiredString(bundle, 'toDate'), maxPages: optionalPositive(bundle, 'maxPages'),
        }
        accounts = (await discoverInvestecAccounts(input.factories.investec(connection), eligibleProductNames)).map(account => ({ ...account, assets: [] }))
        configuration = { eligibleProductNames, fromDate: connection.fromDate, toDate: connection.toDate, ...(connection.maxPages ? { maxPages: connection.maxPages } : {}) }
      } else if (provider === 'mercury') {
        const connection: MercuryRuntimeConnection = {
          ...common(provider, subject), provider: 'mercury', apiToken: requiredString(bundle, 'apiToken'), since: optionalString(bundle, 'since'),
          maxPagesPerAccount: optionalPositive(bundle, 'maxPagesPerAccount'),
        }
        accounts = mercuryAccounts(await input.factories.mercury(connection).fetchAccounts())
        configuration = { ...(connection.since ? { since: connection.since } : {}), ...(connection.maxPagesPerAccount ? { maxPagesPerAccount: connection.maxPagesPerAccount } : {}) }
      } else {
        const connection: SnapTradeRuntimeConnection = {
          ...common(provider, subject), provider: 'snaptrade', clientId: requiredString(bundle, 'clientId'), consumerKey: requiredString(bundle, 'consumerKey'),
          userId: requiredString(bundle, 'userId'), userSecret: requiredString(bundle, 'userSecret'), assetMappings: {},
          activitiesStartDate: optionalString(bundle, 'activitiesStartDate'), activitiesEndDate: optionalString(bundle, 'activitiesEndDate'),
        }
        accounts = snapTradeAccounts(await input.factories.snaptrade(connection).fetchAccounts())
        if (accounts.length === 0) {
          const redirect = await input.snaptrade.createRedirectUri(connection)
          return { status: 'link_ready', redirectUrl: redirect.redirectUri }
        }
        configuration = { ...(connection.activitiesStartDate ? { activitiesStartDate: connection.activitiesStartDate } : {}), ...(connection.activitiesEndDate ? { activitiesEndDate: connection.activitiesEndDate } : {}) }
      }
      if (accounts.length === 0) throw new Error('provider returned no accounts')
      await input.repository.recordConnection({ subject, provider, providerConnectionId, secretReference, configuration, accounts })
      return { status: 'pending' }
    },
  }
}
