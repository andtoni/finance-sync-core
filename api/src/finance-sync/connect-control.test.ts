import { describe, expect, it, vi } from 'vitest'
import {
  CONNECT_PROVIDERS,
  ConnectControlError,
  createSnapTradeConnectTransport,
  FinanceSyncConnectControl,
  type ConnectControlDependencies,
} from './connect-control'
import type { RuntimeConnection } from './runtime'

const subjectA = 'wealthfolio#owner-a'
const subjectB = 'wealthfolio#owner-b'

function connection(provider: 'investec' | 'mercury' | 'snaptrade', localOwnerId = subjectA): RuntimeConnection {
  const common = {
    provider, connectionId: `${provider}-connection`, deploymentId: 'deployment-1', localOwnerId,
    sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['account-1'],
    wealthfolioAccountIds: { 'account-1': 'wealthfolio-account-1' }, accessGeneration: 1,
  }
  if (provider === 'investec') return { ...common, provider, clientId: 'client', clientSecret: 'secret', apiKey: 'key', eligibleProductNames: ['Private Bank'], fromDate: '2026-01-01', toDate: '2026-07-18' }
  if (provider === 'mercury') return { ...common, provider, apiToken: 'token' }
  return { ...common, provider, clientId: 'client', consumerKey: 'consumer', userId: 'user', userSecret: 'secret' }
}

function dependencies(overrides: Partial<ConnectControlDependencies> = {}): ConnectControlDependencies {
  return {
    deploymentId: 'deployment-1',
    connections: [],
    plaid: {
      createLinkToken: vi.fn(async () => ({ linkToken: 'link-token' })),
      exchangePublicToken: vi.fn(async () => ({ accessToken: 'access-secret', itemId: 'item-1' })),
    },
    snaptrade: { createRedirectUri: vi.fn(async () => ({ redirectUri: 'https://app.snaptrade.com/snapTrade/redeemToken?token=short-lived' })) },
    secretStore: { available: true, storePlaidAccessToken: vi.fn(async () => undefined) },
    sessionStore: {
      create: vi.fn(async () => 'continuation-token'),
      consume: vi.fn(async () => true),
    },
    ...overrides,
  }
}

describe('Finance Sync Connect control plane', () => {
  it('uses the typed four-provider allowlist and binds list/status to the authenticated subject', () => {
    expect(CONNECT_PROVIDERS).toEqual(['investec', 'mercury', 'plaid', 'snaptrade'])
    const control = new FinanceSyncConnectControl(dependencies({
      connections: [connection('investec', subjectA), connection('mercury', subjectB)],
      plaid: undefined,
    }))
    expect(control.list(subjectA)).toEqual([
      { provider: 'investec', status: 'configured', message: 'Provider is configured.' },
      { provider: 'mercury', status: 'not_configured', message: 'Provider is not configured.' },
      { provider: 'plaid', status: 'not_configured', message: 'Provider is not configured.' },
      { provider: 'snaptrade', status: 'not_configured', message: 'Provider is not configured.' },
    ])
    expect(control.status(subjectA, 'mercury').status).toBe('not_configured')
    expect(() => control.status(subjectA, 'other')).toThrow('unsupported provider')
  })

  it('returns configured for direct-bank start only with a subject-bound runtime connection', async () => {
    const control = new FinanceSyncConnectControl(dependencies({ connections: [connection('investec', subjectA), connection('mercury', subjectB)] }))
    await expect(control.start(subjectA, 'investec')).resolves.toMatchObject({ provider: 'investec', status: 'configured' })
    await expect(control.start(subjectA, 'mercury')).rejects.toMatchObject({ status: 409 })
  })

  it('creates real provider handoff values only through injected transports', async () => {
    const deps = dependencies({ connections: [connection('snaptrade')] })
    const control = new FinanceSyncConnectControl(deps)
    await expect(control.start(subjectA, 'plaid')).resolves.toMatchObject({ linkToken: 'link-token', continuationToken: 'continuation-token' })
    await expect(control.start(subjectA, 'snaptrade')).resolves.toMatchObject({ redirectUrl: 'https://app.snaptrade.com/snapTrade/redeemToken?token=short-lived' })
    expect(deps.plaid?.createLinkToken).toHaveBeenCalledWith(subjectA)
    expect(deps.snaptrade?.createRedirectUri).toHaveBeenCalledWith(expect.objectContaining({ localOwnerId: subjectA }))
  })

  it('forces SnapTrade portal authorization to read-only and validates the official redirect', async () => {
    const fetchImpl = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(String(input))
      expect(url.searchParams.get('connectionType')).toBe('read')
      return new Response(JSON.stringify({ redirectURI: 'https://app.snaptrade.com/snapTrade/redeemToken?token=short-lived' }), { status: 200 })
    }) as unknown as typeof fetch
    const transport = createSnapTradeConnectTransport({}, fetchImpl)
    const snaptrade = connection('snaptrade') as Extract<RuntimeConnection, { provider: 'snaptrade' }>
    await expect(transport.createRedirectUri(snaptrade)).resolves.toEqual({
      redirectUri: 'https://app.snaptrade.com/snapTrade/redeemToken?token=short-lived',
    })
  })

  it('hands exchanged Plaid access tokens only to ProviderSecretStore and never returns or logs them', async () => {
    const deps = dependencies()
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined)
    const log = vi.spyOn(console, 'log').mockImplementation(() => undefined)
    const result = await new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')
    expect(deps.sessionStore.consume).toHaveBeenCalledWith({ deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', token: 'continuation-token' })
    expect(deps.plaid?.exchangePublicToken).toHaveBeenCalledWith('public-token')
    expect(deps.secretStore.storePlaidAccessToken).toHaveBeenCalledWith({ subject: subjectA, itemId: 'item-1', accessToken: 'access-secret' })
    expect(JSON.stringify(result)).not.toContain('access-secret')
    expect(error).not.toHaveBeenCalled(); expect(log).not.toHaveBeenCalled()
    error.mockRestore(); log.mockRestore()
  })

  it('proves the durable sink writable before consuming or exchanging', async () => {
    const order: string[] = []
    const deps = dependencies({
      secretStore: {
        available: true,
        prepare: vi.fn(async () => { order.push('prepare') }),
        storePlaidAccessToken: vi.fn(async () => { order.push('store') }),
      },
      sessionStore: {
        create: vi.fn(async () => 'continuation-token'),
        consume: vi.fn(async () => { order.push('consume'); return true }),
      },
      plaid: {
        createLinkToken: vi.fn(async () => ({ linkToken: 'link-token' })),
        exchangePublicToken: vi.fn(async () => { order.push('exchange'); return { accessToken: 'access-secret', itemId: 'item-1' } }),
      },
    })
    await new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')
    expect(order).toEqual(['prepare', 'consume', 'exchange', 'store'])
  })

  it('fails before consuming or exchanging when the durable secret store is unavailable', async () => {
    const deps = dependencies({
      secretStore: { available: false, storePlaidAccessToken: vi.fn(async () => undefined) },
    })
    const control = new FinanceSyncConnectControl(deps)
    expect(control.status(subjectA, 'plaid')).toMatchObject({ status: 'not_configured' })
    await expect(control.start(subjectA, 'plaid')).rejects.toMatchObject({ status: 503 })
    await expect(
      control.completePlaid(subjectA, 'public-token', 'continuation-token'),
    ).rejects.toMatchObject({ status: 503 })
    expect(deps.plaid?.createLinkToken).not.toHaveBeenCalled()
    expect(deps.sessionStore.consume).not.toHaveBeenCalled()
    expect(deps.plaid?.exchangePublicToken).not.toHaveBeenCalled()
  })

  it('fails disconnect closed for every allowlisted provider', async () => {
    const control = new FinanceSyncConnectControl(dependencies())
    for (const provider of CONNECT_PROVIDERS) {
      await expect(control.disconnect(subjectA, provider)).rejects.toEqual(expect.objectContaining<Partial<ConnectControlError>>({ status: 501 }))
    }
  })
})
