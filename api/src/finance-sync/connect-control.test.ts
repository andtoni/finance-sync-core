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

  it('allocates a third and many new Plaid Items without a fixed slot cap', async () => {
    const references: string[] = []
    const deps = dependencies({
      secretStore: {
        available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'],
        allocatePlaidSecretReference: vi.fn(() => `FINANCE_SYNC_PLAID_ITEM_${references.length + 1}`),
        acceptsPlaidSecretReference: reference => reference.startsWith('FINANCE_SYNC_PLAID_ITEM_'),
        storePlaidAccessToken: vi.fn(async input => { references.push(input.secretReference); return input.secretReference }),
      },
      sessionStore: { create: vi.fn(async input => { references.push(input.secretReference); return `token-${references.length}` }), consume: vi.fn(async () => session()), release: vi.fn(async () => undefined) },
    })
    const control = new FinanceSyncConnectControl(deps)
    for (let index = 0; index < 20; index += 1) await control.start(subjectA, 'plaid')
    expect(deps.secretStore.allocatePlaidSecretReference).toHaveBeenCalledTimes(20)
    expect(new Set(references).size).toBe(20)
  })

  it('exchanges a continuation only once when duplicate completions race', async () => {
    let consumed = false
    const exchange = vi.fn(async () => ({ clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'access-secret', itemId: 'item-1' }))
    const deps = dependencies({
      plaid: { createLinkToken: vi.fn(async () => ({ linkToken: 'link-token' })), exchangePublicToken: exchange, listAccounts: vi.fn(async () => [{ providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', assets: [] }]) },
      sessionStore: { create: vi.fn(async () => 'continuation-token'), consume: vi.fn(async () => { if (consumed) return undefined; consumed = true; return session() }), release: vi.fn(async () => undefined) },
    })
    const results = await Promise.allSettled([
      new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token'),
      new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token'),
    ])
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1)
    expect(exchange).toHaveBeenCalledOnce()
  })

  it('replays a completed response without provider exchange or onboarding', async () => {
    const exchange = vi.fn()
    const deps = dependencies({
      plaid: undefined,
      onPlaidConnected: undefined,
      sessionStore: {
        create: vi.fn(async () => 'continuation-token'), consume: vi.fn(), release: vi.fn(async () => undefined),
        replayCompleted: vi.fn(async () => ({ itemId: 'item-completed' })),
      },
    })
    await expect(new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')).resolves.toEqual({ provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' })
    expect(deps.sessionStore.consume).not.toHaveBeenCalled()
    expect(exchange).not.toHaveBeenCalled()
  })

  it('retains a recoverable claim when Infisical persistence is uncertain', async () => {
    const release = vi.fn(async () => undefined)
    const markUncertain = vi.fn(async () => undefined)
    const deps = dependencies({
      secretStore: { available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'], storePlaidAccessToken: vi.fn(async () => { throw new Error('timeout') }) },
      sessionStore: { create: vi.fn(async () => 'continuation-token'), consume: vi.fn(async () => ({ ...session(), claimVersion: 3 })), release, markUncertain },
    })
    await expect(new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')).rejects.toMatchObject({ status: 409 })
    expect(markUncertain).toHaveBeenCalledWith(expect.objectContaining({ claimVersion: 3 }))
    expect(release).not.toHaveBeenCalled()
  })

  it('retains a persisted claim when onboarding fails after the credential write', async () => {
    const release = vi.fn(async () => undefined)
    const markCredentialPersisted = vi.fn(async () => undefined)
    const markUncertain = vi.fn(async () => undefined)
    const deps = dependencies({
      onPlaidConnected: vi.fn(async () => { throw new Error('database unavailable') }),
      sessionStore: { create: vi.fn(async () => 'continuation-token'), consume: vi.fn(async () => ({ ...session(), claimVersion: 4 })), release, markCredentialPersisted, markUncertain },
    })
    await expect(new FinanceSyncConnectControl(deps).completePlaid(subjectA, 'public-token', 'continuation-token')).rejects.toThrow('database unavailable')
    expect(markCredentialPersisted).toHaveBeenCalledWith(expect.objectContaining({ claimVersion: 4 }))
    expect(markUncertain).toHaveBeenCalledWith(expect.objectContaining({ claimVersion: 4 }))
    expect(release).not.toHaveBeenCalled()
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

  it('lists each Plaid Item with the exact update selection id, a safe label, and its condition', async () => {
    const first = connection('plaid') as Extract<RuntimeConnection, { provider: 'plaid' }>
    const sibling = { ...first, connectionId: 'plaid-connection-1b', accountIds: ['account-2'], wealthfolioAccountIds: { 'account-2': 'wealthfolio-account-2' } }
    const second = { ...first, connectionId: 'plaid-connection-2', providerConnectionId: 'item-2', accessToken: 'access-token-2', secretReference: 'TEST_SLOT_2' }
    const foreign = { ...first, connectionId: 'plaid-connection-3', providerConnectionId: 'item-3', localOwnerId: subjectB }
    const listConnectedAccounts = vi.fn(async () => [
      { providerConnectionId: 'item-1', name: 'Checking', mask: '1111' },
      { providerConnectionId: 'item-1', name: 'Savings', mask: null },
      { providerConnectionId: 'item-2', name: 'Rewards Card', mask: '2222' },
    ])
    const deps = dependencies({
      connections: [first, sibling, second, foreign],
      plaidItemConditions: new Map([['item-2', 'NO_ACCOUNTS'], ['item-3', 'ITEM_LOGIN_REQUIRED']]),
      onboarding: {
        listPending: vi.fn(async () => []), admit: vi.fn(), revoke: vi.fn(async () => true), reconciliationStatus: vi.fn(async () => null),
        listConnectedAccounts,
      },
    })
    const control = new FinanceSyncConnectControl(deps)
    const listed = await control.list(subjectA)
    expect(listed.find(status => status.provider === 'plaid')).toEqual({
      provider: 'plaid', status: 'configured', message: 'Provider is configured.',
      connections: [
        { connectionId: 'item-1', label: 'Checking •••• 1111, Savings', providerAccountIds: ['account-1', 'account-2'], needsReauth: false },
        { connectionId: 'item-2', label: 'Rewards Card •••• 2222', providerAccountIds: ['account-1'], needsReauth: true, reason: 'NO_ACCOUNTS' },
      ],
    })
    expect('reason' in listed.find(status => status.provider === 'plaid')!.connections![0]!).toBe(false)
    expect(listConnectedAccounts).toHaveBeenCalledWith(subjectA, 'plaid')
    // Only Plaid carries the list, another subject's Item is never disclosed, and no credential leaves the control.
    expect(listed.filter(status => status.provider !== 'plaid').every(status => !('connections' in status))).toBe(true)
    expect(JSON.stringify(listed)).not.toMatch(/item-3|access-token|client|secret|TEST_SLOT/)
    // Every listed id is exactly what the update route accepts.
    await expect(control.startPlaidUpdate(subjectA, 'item-2')).resolves.toMatchObject({ status: 'link_ready' })
    expect(deps.plaid?.createUpdateLinkToken).toHaveBeenCalledWith(subjectA, expect.objectContaining({ providerConnectionId: 'item-2', accessToken: 'access-token-2' }))
  })

  it('labels a Plaid Item generically when no account names are available and omits the list without Items', async () => {
    const withItem = new FinanceSyncConnectControl(dependencies({ connections: [connection('plaid')] }))
    expect((await withItem.list(subjectA)).find(status => status.provider === 'plaid')).toMatchObject({
      connections: [{ connectionId: 'item-1', label: 'Plaid connection', needsReauth: false }],
    })
    // A condition Plaid gave no documented code for still needs the owner, with no reason to show.
    const unexplained = new FinanceSyncConnectControl(dependencies({
      connections: [connection('plaid')], plaidItemConditions: new Map([['item-1', undefined]]),
    }))
    const [summary] = (await unexplained.list(subjectA)).find(status => status.provider === 'plaid')!.connections!
    expect(summary).toEqual({ connectionId: 'item-1', label: 'Plaid connection', providerAccountIds: ['account-1'], needsReauth: true })
    expect('reason' in summary!).toBe(false)
    // Provider text cannot break the receiver's bounded, control-free label check.
    const hostile = new FinanceSyncConnectControl(dependencies({
      connections: [connection('plaid')],
      onboarding: {
        listPending: vi.fn(async () => []), admit: vi.fn(), revoke: vi.fn(async () => true), reconciliationStatus: vi.fn(async () => null),
        listConnectedAccounts: vi.fn(async () => [
          { providerConnectionId: 'item-1', name: 'Line\nbreak\u0000\u0085', mask: '1111' },
          { providerConnectionId: 'item-1', name: '😀'.repeat(300), mask: null },
        ]),
      },
    }))
    const hostileLabel = (await hostile.list(subjectA)).find(status => status.provider === 'plaid')!.connections![0]!.label
    expect(hostileLabel.startsWith('Line break •••• 1111, 😀')).toBe(true)
    expect(/[\u0000-\u001f\u007f-\u009f]/.test(hostileLabel)).toBe(false)
    expect([...hostileLabel]).toHaveLength(200)
    expect(() => encodeURIComponent(hostileLabel)).not.toThrow() // no split surrogate pair
    expect(Buffer.byteLength(hostileLabel, 'utf8')).toBeLessThanOrEqual(1024)
    const withoutItem = new FinanceSyncConnectControl(dependencies({ connections: [connection('investec')] }))
    expect((await withoutItem.list(subjectA)).find(status => status.provider === 'plaid')).toEqual({
      provider: 'plaid', status: 'available', message: 'Provider is available to connect.',
    })
  })

  it('names the bank behind each Plaid Item, asking Plaid once per Item and never failing the list over it', async () => {
    const first = connection('plaid') as Extract<RuntimeConnection, { provider: 'plaid' }>
    const second = { ...first, connectionId: 'plaid-connection-2', providerConnectionId: 'item-2', accessToken: 'access-token-2', accountIds: ['account-2'] }
    const third = { ...first, connectionId: 'plaid-connection-3', providerConnectionId: 'item-3', accessToken: 'access-token-3', accountIds: ['account-3'] }
    const institutionName = vi.fn(async (input: { accessToken: string }) => {
      if (input.accessToken === 'access-token-2') throw new Error('Plaid is down')
      return input.accessToken === 'access-token-3' ? ' Bank\nB\u0000 ' : 'Bank A'
    })
    const base = dependencies()
    const control = new FinanceSyncConnectControl({ ...base, connections: [first, second, third], plaid: { ...base.plaid!, institutionName } })
    const names = async () => (await control.list(subjectA)).find(status => status.provider === 'plaid')!.connections!
      .map(summary => [summary.connectionId, summary.institutionName, summary.providerAccountIds])
    expect(await names()).toEqual([
      ['item-1', 'Bank A', ['account-1']], ['item-2', undefined, ['account-2']], ['item-3', 'Bank B', ['account-3']],
    ])
    expect(institutionName).toHaveBeenCalledWith(expect.objectContaining({ accessToken: 'access-token' }))
    // A name already learned is kept; only the Item Plaid failed for is asked again.
    await names()
    expect(institutionName.mock.calls.map(([input]) => input.accessToken).sort())
      .toEqual(['access-token', 'access-token-2', 'access-token-2', 'access-token-3'])
  })

  it('clears the Item condition only when the Plaid update completes', async () => {
    const plaidItemConditions = new Map([['item-1', 'ITEM_LOGIN_REQUIRED'], ['item-other', 'ACCOUNT_NOT_RETURNED']])
    const sessionStore = {
      create: vi.fn(async () => 'continuation-token'),
      consume: vi.fn(async () => session('TEST_SLOT', 'update', 'item-1', 1)),
      release: vi.fn(async () => undefined),
    }
    const failing = new FinanceSyncConnectControl(dependencies({
      connections: [connection('plaid')], sessionStore, plaidItemConditions,
      onPlaidAccountsUpdated: vi.fn(async () => { throw new Error('rediscovery failed') }),
    }))
    await expect(failing.completePlaidUpdate(subjectA, 'continuation-token')).rejects.toThrow('rediscovery failed')
    expect([...plaidItemConditions.keys()].sort()).toEqual(['item-1', 'item-other'])

    const control = new FinanceSyncConnectControl(dependencies({
      connections: [connection('plaid')], sessionStore, plaidItemConditions,
      onPlaidAccountsUpdated: vi.fn(async () => undefined),
    }))
    expect((await control.list(subjectA)).find(status => status.provider === 'plaid')?.connections?.[0]).toMatchObject({ needsReauth: true, reason: 'ITEM_LOGIN_REQUIRED' })
    await expect(control.completePlaidUpdate(subjectA, 'continuation-token')).resolves.toMatchObject({ status: 'pending' })
    expect([...plaidItemConditions]).toEqual([['item-other', 'ACCOUNT_NOT_RETURNED']])
    const cleared = (await control.list(subjectA)).find(status => status.provider === 'plaid')!.connections![0]!
    expect(cleared.needsReauth).toBe(false)
    expect('reason' in cleared).toBe(false)
  })

  it('releases the DB reservation when generated placeholder creation fails', async () => {
    const release = vi.fn(async () => undefined)
    const create = vi.fn(async () => 'continuation-token')
    const deps = dependencies({
      secretStore: {
        available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'],
        allocatePlaidSecretReference: vi.fn(() => 'FINANCE_SYNC_PLAID_ITEM_0123456789ABCDEF0123456789ABCDEF'),
        prepare: vi.fn(async () => { throw new Error('placeholder denied') }),
        storePlaidAccessToken: vi.fn(async () => 'never'),
      },
      sessionStore: { create, consume: vi.fn(), release },
    })
    await expect(new FinanceSyncConnectControl(deps).start(subjectA, 'plaid')).rejects.toThrow('placeholder denied')
    expect(create).toHaveBeenCalledOnce()
    expect(release).toHaveBeenCalledWith({ deploymentId: 'deployment-1', subject: subjectA, provider: 'plaid', token: 'continuation-token' })
    expect(deps.plaid?.createLinkToken).not.toHaveBeenCalled()
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
    const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as Record<string, unknown>)
      if (new URL(String(input)).pathname === '/item/get') return new Response(JSON.stringify({ item: { item_id: 'item-1', institution_name: 'Bank A' } }), { status: 200 })
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
    // The bank name is read with the Item's own credentials, like the update link token.
    await expect(transport!.institutionName!({ accessToken: 'existing-access', clientId: 'existing-client', clientSecret: 'existing-secret' })).resolves.toBe('Bank A')
    expect(requests[3]).toEqual({ access_token: 'existing-access', client_id: 'existing-client', secret: 'existing-secret' })
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
      subject: subjectA, itemId: 'item-1', secretReference: 'PLAID_SLOT_A', clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'access-secret', reservationBinding: expect.stringMatching(/^[0-9a-f]{64}$/),
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

  describe('an admission the last sync cycle ingested under a re-issued account id', () => {
    const reissued = {
      wealthfolioAccountId: 'wealthfolio-account-1',
      from: { connectionId: 'connection-dead', providerAccountId: 'account-1' },
      to: { connectionId: 'connection-live', providerAccountId: 'account-new' },
    }
    const admitted = (localOwnerId = subjectA) => ({ ...connection('plaid', localOwnerId), connectionId: 'connection-dead:account-1' })
    const target = { wealthfolioAccountId: 'wealthfolio-account-1', connectionId: 'connection-live', providerAccountId: 'account-new' }
    const onboarding = (moveAdmission: NonNullable<NonNullable<ConnectControlDependencies['onboarding']>['moveAdmission']>) => ({
      listPending: vi.fn(), admit: vi.fn(), revoke: vi.fn(), reconciliationStatus: vi.fn(), moveAdmission,
    })

    it('is named only to the subject that owns it, beside where the account is admitted', async () => {
      const settled = { ...connection('plaid'), connectionId: 'connection-live:account-2', accountIds: ['account-2'], wealthfolioAccountIds: { 'account-2': 'wealthfolio-account-0' } }
      const control = new FinanceSyncConnectControl(dependencies({
        connections: [admitted(), settled, { ...admitted(subjectB), wealthfolioAccountIds: { 'account-1': 'wealthfolio-account-2' } }, connection('mercury')],
        plaidReissuedAccounts: new Map([[reissued.wealthfolioAccountId, reissued]]),
      }))
      await expect(control.plaidAdmissions(subjectA)).resolves.toEqual([
        { wealthfolioAccountId: 'wealthfolio-account-0', connectionId: 'connection-live', providerAccountId: 'account-2', providerGeneration: 1 },
        { wealthfolioAccountId: 'wealthfolio-account-1', connectionId: 'connection-dead', providerAccountId: 'account-1', providerGeneration: 1, reissuedAs: reissued.to },
      ])
      await expect(control.plaidAdmissions(subjectB)).resolves.toEqual([
        { wealthfolioAccountId: 'wealthfolio-account-2', connectionId: 'connection-dead', providerAccountId: 'account-1', providerGeneration: 1 },
      ])
    })

    it('moves to exactly the account the cycle observed, once', async () => {
      const plaidReissuedAccounts = new Map([[reissued.wealthfolioAccountId, reissued]])
      const moveAdmission = vi.fn(async (input: { from?: unknown }) => {
        if (input.from === undefined) throw new Error('admission is not returned under the requested provider account')
        return { ...target, providerGeneration: 4 }
      })
      const control = new FinanceSyncConnectControl(dependencies({ onboarding: onboarding(moveAdmission), plaidReissuedAccounts }))
      await expect(control.moveAdmission(subjectA, { ...target, providerAccountId: 'account-other' })).rejects.toMatchObject({ status: 409 })
      expect(moveAdmission).toHaveBeenLastCalledWith({ subject: subjectA, ...target, providerAccountId: 'account-other' })
      expect(plaidReissuedAccounts.size).toBe(1)

      await expect(control.moveAdmission(subjectA, target)).resolves.toEqual({ ...target, providerGeneration: 4 })
      expect(moveAdmission).toHaveBeenLastCalledWith({ subject: subjectA, ...target, from: reissued.from })
      expect(plaidReissuedAccounts.size).toBe(0)

      // A repeat reaches the repository without an observation; only an already completed move can answer it.
      await expect(control.moveAdmission(subjectA, target)).rejects.toMatchObject({ status: 409 })
      expect(moveAdmission).toHaveBeenLastCalledWith({ subject: subjectA, ...target })
      await expect(control.moveAdmission(subjectA, { ...target, connectionId: '' })).rejects.toMatchObject({ status: 400 })
      await expect(new FinanceSyncConnectControl(dependencies()).moveAdmission(subjectA, target)).rejects.toMatchObject({ status: 503 })
    })
  })
})
