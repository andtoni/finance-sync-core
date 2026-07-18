import { describe, expect, it, vi } from 'vitest'
import {
  CONNECT_PROVIDERS,
  ConnectControlError,
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
    connections: [],
    plaid: {
      createLinkToken: vi.fn(async () => ({ linkToken: 'link-token' })),
      exchangePublicToken: vi.fn(async () => ({ accessToken: 'access-secret', itemId: 'item-1' })),
    },
    snaptrade: { createRedirectUri: vi.fn(async () => ({ redirectUri: 'https://snaptrade.example/redirect' })) },
    secretStore: { storePlaidAccessToken: vi.fn(async () => undefined) },
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
    await expect(control.start(subjectA, 'plaid')).resolves.toMatchObject({ linkToken: 'link-token' })
    await expect(control.start(subjectA, 'snaptrade')).resolves.toMatchObject({ redirectUrl: 'https://snaptrade.example/redirect' })
    expect(deps.plaid?.createLinkToken).toHaveBeenCalledWith(subjectA)
    expect(deps.snaptrade?.createRedirectUri).toHaveBeenCalledWith(expect.objectContaining({ localOwnerId: subjectA }))
  })

  it('hands exchanged Plaid access tokens only to ProviderSecretStore and never returns or logs them', async () => {
    const deps = dependencies()
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined)
    const log = vi.spyOn(console, 'log').mockImplementation(() => undefined)
    const result = await new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token')
    expect(deps.plaid?.exchangePublicToken).toHaveBeenCalledWith('public-token')
    expect(deps.secretStore.storePlaidAccessToken).toHaveBeenCalledWith({ subject: subjectA, itemId: 'item-1', accessToken: 'access-secret' })
    expect(JSON.stringify(result)).not.toContain('access-secret')
    expect(error).not.toHaveBeenCalled(); expect(log).not.toHaveBeenCalled()
    error.mockRestore(); log.mockRestore()
  })

  it('fails disconnect closed for every allowlisted provider', async () => {
    const control = new FinanceSyncConnectControl(dependencies())
    for (const provider of CONNECT_PROVIDERS) {
      await expect(control.disconnect(subjectA, provider)).rejects.toEqual(expect.objectContaining<Partial<ConnectControlError>>({ status: 501 }))
    }
  })
})
