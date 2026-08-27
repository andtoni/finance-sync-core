import { describe, expect, it, vi } from 'vitest'
import {
  CONNECT_PROVIDERS,
  ConnectControlError,
  createPlaidConnectTransport,
  createSnapTradeConnectTransport,
  FinanceSyncConnectControl,
  type ConnectControlDependencies,
  type ConsumedPlaidConnectSession,
} from './connect-control'
import type { RuntimeConnection } from './runtime'

const subjectA = 'wealthfolio#owner-a'
const subjectB = 'wealthfolio#owner-b'

function session(secretReference = 'PLAID_SLOT_A', flow: 'new' | 'update' = 'new', providerConnectionId?: string, providerGeneration?: number): ConsumedPlaidConnectSession {
  return { secretReference, flow, ...(providerConnectionId === undefined ? {} : { providerConnectionId }), ...(providerGeneration === undefined ? {} : { providerGeneration }) }
}

function connection(provider: 'investec' | 'mercury' | 'plaid' | 'snaptrade', localOwnerId = subjectA): RuntimeConnection {
  const common = {
    provider, connectionId: `${provider}-connection`, deploymentId: 'deployment-1', localOwnerId,
    sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['account-1'],
    wealthfolioAccountIds: { 'account-1': 'wealthfolio-account-1' }, providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT',
  }
  if (provider === 'investec') return { ...common, provider, clientId: 'client', clientSecret: 'secret', apiKey: 'key', eligibleProductNames: ['Private Bank'], fromDate: '2026-01-01', toDate: '2026-07-18' }
  if (provider === 'mercury') return { ...common, provider, apiToken: 'token' }
  if (provider === 'plaid') return { ...common, provider, providerConnectionId: 'item-1', clientId: 'client', clientSecret: 'secret', accessToken: 'access-token' }
  return { ...common, provider, clientId: 'client', consumerKey: 'consumer', authMode: 'commercial', userId: 'user', userSecret: 'secret', assetMappings: {} }
}

function dependencies(overrides: Partial<ConnectControlDependencies> = {}): ConnectControlDependencies {
  return {
    deploymentId: 'deployment-1',
    connections: [],
    plaid: {
      createLinkToken: vi.fn(async () => ({ linkToken: 'link-token' })),
      createUpdateLinkToken: vi.fn(async () => ({ linkToken: 'update-link-token' })),
      exchangePublicToken: vi.fn(async () => ({ clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'access-secret', itemId: 'item-1' })),
      listAccounts: vi.fn(async () => [{ providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', assets: [] }]),
    },
    snaptrade: { createRedirectUri: vi.fn(async () => ({ redirectUri: 'https://app.snaptrade.com/snapTrade/redeemToken?token=short-lived' })) },
    secretStore: {
      available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'],
      storePlaidAccessToken: vi.fn(async () => 'PLAID_SLOT_A'),
    },
    sessionStore: {
      create: vi.fn(async () => 'continuation-token'),
      consume: vi.fn(async () => session()),
      release: vi.fn(async () => undefined),
    },
    onPlaidConnected: vi.fn(async () => undefined),
    ...overrides,
  }
}

describe('Finance Sync Connect control plane', () => {
  it('uses the typed four-provider allowlist and binds list/status to the authenticated subject', async () => {
    expect(CONNECT_PROVIDERS).toEqual(['investec', 'mercury', 'plaid', 'snaptrade'])
    const control = new FinanceSyncConnectControl(dependencies({
      connections: [connection('investec', subjectA), connection('mercury', subjectB)],
      plaid: undefined,
    }))
    expect(await control.list(subjectA)).toEqual([
      { provider: 'investec', status: 'configured', message: 'Provider is configured.' },
      { provider: 'mercury', status: 'not_configured', message: 'Provider is not configured.' },
      { provider: 'plaid', status: 'not_configured', message: 'Provider is not configured.' },
      { provider: 'snaptrade', status: 'not_configured', message: 'Provider is not configured.' },
    ])
    expect((await control.status(subjectA, 'mercury')).status).toBe('not_configured')
    await expect(control.status(subjectA, 'other')).rejects.toThrow('unsupported provider')
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

  it('opens Plaid Link update mode for the existing Item without allocating a new slot', async () => {
    const deps = dependencies({ connections: [connection('plaid')] })
    const result = await new FinanceSyncConnectControl(deps).startPlaidUpdate(subjectA)
    expect(result).toMatchObject({ provider: 'plaid', status: 'link_ready', linkToken: 'update-link-token', continuationToken: 'continuation-token' })
    expect(deps.plaid?.createUpdateLinkToken).toHaveBeenCalledWith(subjectA, expect.objectContaining({
      providerConnectionId: 'item-1', accessToken: 'access-token', clientId: 'client', clientSecret: 'secret',
    }))
    expect(deps.sessionStore.create).toHaveBeenCalledWith({
      deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', secretReference: 'TEST_SLOT',
      providerConnectionId: 'item-1', providerGeneration: 1, flow: 'update',
    })
    expect(deps.plaid?.exchangePublicToken).not.toHaveBeenCalled()
  })

  it('rediscovers accounts after Plaid update without rotating the Item', async () => {
    const updated = vi.fn(async () => undefined)
    const deps = dependencies({
      connections: [connection('plaid')],
      sessionStore: {
        create: vi.fn(async () => 'continuation-token'),
        consume: vi.fn(async () => session('TEST_SLOT', 'update', 'item-1', 1)),
        release: vi.fn(async () => undefined),
      },
      onPlaidAccountsUpdated: updated,
    })
    await expect(new FinanceSyncConnectControl(deps).completePlaidUpdate(subjectA, 'continuation-token')).resolves.toMatchObject({ status: 'pending' })
    expect(deps.sessionStore.consume).toHaveBeenCalledWith({ deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', token: 'continuation-token', flow: 'update' })
    expect(deps.plaid?.listAccounts).toHaveBeenCalledWith('access-token')
    expect(updated).toHaveBeenCalledWith({
      subject: subjectA, providerConnectionId: 'item-1', secretReference: 'TEST_SLOT', providerGeneration: 1,
      accounts: [{ providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', assets: [] }],
    })
    expect(deps.plaid?.exchangePublicToken).not.toHaveBeenCalled()
    expect(deps.secretStore.storePlaidAccessToken).not.toHaveBeenCalled()
  })

  it('rejects an update continuation when the provider generation changes', async () => {
    const updated = vi.fn(async () => undefined)
    const connectionWithNewGeneration = { ...connection('plaid'), providerGeneration: 2 }
    const deps = dependencies({
      connections: [connectionWithNewGeneration],
      sessionStore: {
        create: vi.fn(async () => 'continuation-token'),
        consume: vi.fn(async () => session('TEST_SLOT', 'update', 'item-1', 1)),
        release: vi.fn(async () => undefined),
      },
      onPlaidAccountsUpdated: updated,
    })
    await expect(new FinanceSyncConnectControl(deps).completePlaidUpdate(subjectA, 'continuation-token')).rejects.toMatchObject({ status: 409 })
    expect(deps.plaid?.listAccounts).not.toHaveBeenCalled()
    expect(updated).not.toHaveBeenCalled()
  })

  it('fails closed when more than one Plaid Item needs a connection selection', async () => {
    const first = connection('plaid') as Extract<RuntimeConnection, { provider: 'plaid' }>
    const second = { ...first, connectionId: 'plaid-connection-2', providerConnectionId: 'item-2' }
    await expect(new FinanceSyncConnectControl(dependencies({ connections: [first, second] })).startPlaidUpdate(subjectA)).rejects.toMatchObject({ status: 409 })
  })

  it('releases the reserved slot when Plaid Link bootstrap fails', async () => {
    const release = vi.fn(async () => undefined)
    const deps = dependencies({
      plaid: { createLinkToken: vi.fn(async () => { throw new Error('Link failed') }), exchangePublicToken: vi.fn(), listAccounts: vi.fn() },
      sessionStore: { create: vi.fn(async () => 'continuation-token'), consume: vi.fn(async () => session()), release },
    })
    await expect(new FinanceSyncConnectControl(deps).start(subjectA, 'plaid')).rejects.toThrow('Link failed')
    expect(release).toHaveBeenCalledWith({ deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', token: 'continuation-token' })
  })

  it('requires Plaid transactions and requests liabilities and investments as additional consent', async () => {
    const requests: Record<string, unknown>[] = []
    const fetchImpl = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as Record<string, unknown>)
      return new Response(JSON.stringify(requests.length <= 2
        ? { link_token: 'link-token' }
        : { accounts: [{ account_id: 'account-1', name: 'Checking', subtype: 'checking', mask: '1234', balances: { iso_currency_code: 'USD' } }] }), { status: 200 })
    }) as unknown as typeof fetch
    const resolve = vi.fn(async () => ({ clientId: 'client', clientSecret: 'secret' }))
    const transport = createPlaidConnectTransport({
      // Direct Plaid credential environment inputs are deliberately not part of this interface.
      env: {}, secretReference: 'PLAID_CONNECT', secretReader: { available: true, resolve },
    }, fetchImpl)
    expect(transport).toBeDefined()
    expect(resolve).not.toHaveBeenCalled()
    await transport!.createLinkToken(subjectA)
    expect(resolve).toHaveBeenCalledOnce()
    expect(resolve).toHaveBeenCalledWith('PLAID_CONNECT')
    await expect(transport!.createUpdateLinkToken!(subjectA, { accessToken: 'existing-access', clientId: 'existing-client', clientSecret: 'existing-secret' })).resolves.toEqual({ linkToken: 'link-token' })
    expect(requests[1]).toMatchObject({
      access_token: 'existing-access',
      update: { account_selection_enabled: true },
      user: { client_user_id: subjectA },
      client_id: 'existing-client',
      secret: 'existing-secret',
    })
    await expect(transport!.listAccounts('access-token')).resolves.toEqual([
      { providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', mask: '1234', assets: [] },
    ])
    expect(requests[0]?.products).toEqual(['transactions'])
    expect(requests[0]?.additional_consented_products).toEqual(['liabilities', 'investments'])
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

  it('omits subordinate user credentials for a Personal SnapTrade key', async () => {
    const fetchImpl = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(String(input))
      expect(url.searchParams.get('userId')).toBeNull()
      expect(url.searchParams.get('userSecret')).toBeNull()
      return new Response(JSON.stringify({ redirectURI: 'https://app.snaptrade.com/snapTrade/redeemToken?token=short-lived' }), { status: 200 })
    }) as unknown as typeof fetch
    const transport = createSnapTradeConnectTransport({}, fetchImpl)
    const commercial = connection('snaptrade') as Extract<RuntimeConnection, { provider: 'snaptrade' }>
    const personal = { ...commercial, authMode: 'personal' as const }
    await expect(transport.createRedirectUri(personal)).resolves.toMatchObject({ redirectUri: expect.stringContaining('redeemToken') })
  })

  it('hands exchanged Plaid access tokens only to ProviderSecretStore and never returns or logs them', async () => {
    const deps = dependencies()
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined)
    const log = vi.spyOn(console, 'log').mockImplementation(() => undefined)
    const result = await new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')
    expect(deps.sessionStore.consume).toHaveBeenCalledWith({ deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', token: 'continuation-token', flow: 'new' })
    expect(deps.plaid?.exchangePublicToken).toHaveBeenCalledWith('public-token')
    expect(deps.secretStore.storePlaidAccessToken).toHaveBeenCalledWith({
      subject: subjectA, itemId: 'item-1', secretReference: 'PLAID_SLOT_A', clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'access-secret',
    })
    expect(JSON.stringify(result)).not.toContain('access-secret')
    expect(error).not.toHaveBeenCalled(); expect(log).not.toHaveBeenCalled()
    error.mockRestore(); log.mockRestore()
  })

  it('proves the durable sink writable before consuming or exchanging', async () => {
    const order: string[] = []
    const deps = dependencies({
      secretStore: {
        available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'],
        prepare: vi.fn(async () => { order.push('prepare') }),
        storePlaidAccessToken: vi.fn(async () => { order.push('store'); return 'PLAID_SLOT_A' }),
      },
      sessionStore: {
        create: vi.fn(async () => 'continuation-token'),
        consume: vi.fn(async () => { order.push('consume'); return session() }),
        release: vi.fn(async () => undefined),
      },
      plaid: {
        createLinkToken: vi.fn(async () => ({ linkToken: 'link-token' })),
        exchangePublicToken: vi.fn(async () => {
          order.push('exchange'); return { clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'access-secret', itemId: 'item-1' }
        }),
        listAccounts: vi.fn(async () => { order.push('discover'); return [{ providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', assets: [] }] }),
      },
      onPlaidConnected: vi.fn(async () => { order.push('record') }),
    })
    await new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')
    expect(order).toEqual(['prepare', 'consume', 'exchange', 'store', 'discover', 'record'])
  })

  it('does not issue or exchange Plaid tokens without durable onboarding', async () => {
    const deps = dependencies({ onPlaidConnected: undefined })
    const control = new FinanceSyncConnectControl(deps)
    await expect(control.status(subjectA, 'plaid')).resolves.toMatchObject({ status: 'not_configured' })
    await expect(control.start(subjectA, 'plaid')).rejects.toMatchObject({ status: 503 })
    await expect(control.completePlaid(subjectA, 'public-token', 'continuation-token')).rejects.toMatchObject({ status: 503 })
    expect(deps.sessionStore.consume).not.toHaveBeenCalled()
    expect(deps.plaid?.exchangePublicToken).not.toHaveBeenCalled()
  })

  it('fails before consuming or exchanging when the durable secret store is unavailable', async () => {
    const deps = dependencies({
      secretStore: {
        available: false, allowedSlots: [],
        storePlaidAccessToken: vi.fn(async () => 'never'),
      },
    })
    const control = new FinanceSyncConnectControl(deps)
    await expect(control.status(subjectA, 'plaid')).resolves.toMatchObject({ status: 'not_configured' })
    await expect(control.start(subjectA, 'plaid')).rejects.toMatchObject({ status: 503 })
    await expect(
      control.completePlaid(subjectA, 'public-token', 'continuation-token'),
    ).rejects.toMatchObject({ status: 503 })
    expect(deps.plaid?.createLinkToken).not.toHaveBeenCalled()
    expect(deps.sessionStore.consume).not.toHaveBeenCalled()
    expect(deps.plaid?.exchangePublicToken).not.toHaveBeenCalled()
  })

  it('binds a selected rotation slot to its continuation and excludes the active slot', async () => {
    const old: RuntimeConnection = {
      provider: 'plaid', connectionId: 'old', deploymentId: 'deployment-1', localOwnerId: subjectA,
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['account-1'],
      wealthfolioAccountIds: { 'account-1': 'wf-1' }, providerGeneration: 1, receiverAccessGeneration: 1,
      secretReference: 'PLAID_SLOT_A', clientId: 'client', clientSecret: 'secret', accessToken: 'old',
    }
    const deps = dependencies({ connections: [old], sessionStore: {
      create: vi.fn(async () => 'continuation-token'), consume: vi.fn(async () => session('PLAID_SLOT_B')), release: vi.fn(async () => undefined),
    }, secretStore: { available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'], storePlaidAccessToken: vi.fn(async () => 'PLAID_SLOT_B') } })
    await new FinanceSyncConnectControl(deps).start(subjectA, 'plaid')
    expect(deps.sessionStore.create).toHaveBeenCalledWith({ deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', secretReference: 'PLAID_SLOT_B', flow: 'new' })
    await new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')
    expect(deps.secretStore.storePlaidAccessToken).toHaveBeenCalledWith(expect.objectContaining({ secretReference: 'PLAID_SLOT_B' }))
  })

  it('excludes active slots owned by another authenticated subject', async () => {
    const otherSubjectConnection: RuntimeConnection = {
      provider: 'plaid', connectionId: 'other-owner', deploymentId: 'deployment-1', localOwnerId: subjectB,
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['account-1'],
      wealthfolioAccountIds: { 'account-1': 'wf-1' }, providerGeneration: 1, receiverAccessGeneration: 1,
      secretReference: 'PLAID_SLOT_A', clientId: 'client', clientSecret: 'secret', accessToken: 'active',
    }
    const deps = dependencies({ connections: [otherSubjectConnection] })
    await new FinanceSyncConnectControl(deps).start(subjectA, 'plaid')
    expect(deps.sessionStore.create).toHaveBeenCalledOnce()
    expect(deps.sessionStore.create).toHaveBeenCalledWith(expect.objectContaining({ subject: subjectA, secretReference: 'PLAID_SLOT_B' }))
  })

  it('rejects a syntactically valid continuation slot outside the writer allowlist', async () => {
    const deps = dependencies({ sessionStore: { create: vi.fn(async () => 'continuation-token'), consume: vi.fn(async () => session('PLAID_SLOT_C')), release: vi.fn(async () => undefined) } })
    await expect(new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')).rejects.toMatchObject({ status: 409 })
    expect(deps.plaid?.exchangePublicToken).not.toHaveBeenCalled()
  })

  it('rejects a writer response that changes the continuation-bound slot', async () => {
    const deps = dependencies({ secretStore: { available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'], storePlaidAccessToken: vi.fn(async () => 'PLAID_SLOT_B') } })
    await expect(new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')).rejects.toMatchObject({ status: 502 })
    expect(deps.onPlaidConnected).not.toHaveBeenCalled()
  })

  it('rejects allocation when all slots are active', async () => {
    const active = ['PLAID_SLOT_A', 'PLAID_SLOT_B'].map((secretReference, index) => ({
      provider: 'plaid' as const, connectionId: `old-${index}`, deploymentId: 'deployment-1', localOwnerId: subjectA,
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['account-1'], wealthfolioAccountIds: { 'account-1': 'wf-1' },
      providerGeneration: 1, receiverAccessGeneration: 1, secretReference, clientId: 'client', clientSecret: 'secret', accessToken: 'old',
    }))
    await expect(new FinanceSyncConnectControl(dependencies({ connections: active })).start(subjectA, 'plaid')).rejects.toMatchObject({ status: 409 })
  })

  it('does not replace the active generation reference when onboarding fails after slot write', async () => {
    const oldReference = 'PLAID_SLOT_A'
    const onPlaidConnected = vi.fn(async () => { throw new Error('post-write failure') })
    const deps = dependencies({ onPlaidConnected })
    await expect(new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')).rejects.toThrow('post-write failure')
    expect(oldReference).toBe('PLAID_SLOT_A')
    expect(deps.secretStore.storePlaidAccessToken).toHaveBeenCalledWith(expect.objectContaining({ secretReference: 'PLAID_SLOT_A' }))
  })

  it('generation-fences local disconnects without a provider write', async () => {
    const revoke = vi.fn(async (boundSubject: string, provider: string, connectionId: string) => boundSubject === subjectA && provider === 'plaid' && connectionId === 'connection-1')
    const control = new FinanceSyncConnectControl(dependencies({ onboarding: {
      listPending: vi.fn(), admit: vi.fn(), revoke, reconciliationStatus: vi.fn(),
    } }))
    await expect(control.disconnect(subjectA, 'plaid', 'connection-1')).resolves.toMatchObject({ provider: 'plaid', status: 'disconnected' })
    expect(revoke).toHaveBeenCalledWith(subjectA, 'plaid', 'connection-1')
    await expect(control.disconnect(subjectA, 'mercury', 'connection-1')).rejects.toMatchObject({ status: 404 })
  })
})
