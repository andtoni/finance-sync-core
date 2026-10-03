import { describe, expect, it } from 'vitest'
import { createProviderTransportFactories, parseRuntimeConnections, runRuntimeProvider, type ProviderTransportFactories } from './provider-runtime'

const common = {
  connectionId: 'connection-1', deploymentId: 'dep-1', localOwnerId: 'urn:test#owner',
  sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
  accountIds: ['account-1'], wealthfolioAccountIds: { 'account-1': 'wf-account-1' },
  providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT',
}
const configurations = [
  { ...common, provider: 'plaid' },
  { ...common, connectionId: 'connection-2', provider: 'investec', clientId: 'id', eligibleProductNames: ['Private Bank'], fromDate: '2026-01-01', toDate: '2026-07-18' },
  { ...common, connectionId: 'connection-3', provider: 'mercury', since: '2026-01-01' },
  { ...common, connectionId: 'connection-4', provider: 'snaptrade', clientId: 'id', userId: 'user', assetMappings: {}, activitiesStartDate: '2026-01-01', activitiesEndDate: '2026-07-18' },
]
const providerSecrets = {
  'connection-1': { clientId: 'client', clientSecret: 'secret', accessToken: 'server-token' },
  'connection-2': { clientSecret: 'secret', apiKey: 'key' },
  'connection-3': { apiToken: 'read-token' },
  'connection-4': { consumerKey: 'key', userSecret: 'durable-secret' },
}
function runtimeEnv(values: readonly Record<string, unknown>[] = configurations) {
  return {
    FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify(values),
    FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify(providerSecrets),
  }
}

function factories(): ProviderTransportFactories {
  return {
    plaid: () => ({
      getAccounts: async () => [{ account_id: 'account-1', name: 'Cash', type: 'depository', subtype: 'checking', balances: { current: 1, iso_currency_code: 'USD' } }],
      syncTransactions: async () => ({ added: [], modified: [], removed: [], next_cursor: 'plaid-next', has_more: false }),
    }),
    investec: () => ({
      listAccounts: async () => [{ accountId: 'account-1', productName: 'Private Bank' }],
      getBalance: async () => ({ currency: 'ZAR', currentBalance: 1 }),
      getTransactionPage: async () => ({ transactions: [], nextCursor: undefined }),
    }),
    mercury: () => ({
      fetchAccounts: async () => ({ accounts: [{ id: 'account-1', name: 'Operating', type: 'cash', currentBalance: 1, availableBalance: 1, currency: 'USD', status: 'active' }] }),
      fetchTransactions: async () => ({ transactions: [], total: 0 }),
    }),
    snaptrade: () => ({
      fetchAccounts: async () => [{ account: { id: 'account-1', name: 'Brokerage' }, positions: [], total_value: { value: 1, currency: 'USD' } }],
      fetchActivities: async () => [],
    }),
  }
}

describe('four-provider runtime configuration', () => {
  it('parses exactly the allowlisted providers into provider-specific server configs', () => {
    const parsed = parseRuntimeConnections(runtimeEnv(), 'dep-1')
    expect(parsed.map(connection => connection.provider)).toEqual(['plaid', 'investec', 'mercury', 'snaptrade'])
    expect(parsed[1]).toMatchObject({ provider: 'investec', apiKey: 'key', eligibleProductNames: ['Private Bank'] })
    expect(parsed[3]).toMatchObject({ provider: 'snaptrade', userSecret: 'durable-secret' })
  })

  it.each(['lunchflow', 'unknown'])('fails closed for provider %s', provider => {
    expect(() => parseRuntimeConnections({ FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([{ ...common, provider }]) }, 'dep-1')).toThrow('unsupported runtime provider')
  })

  it('accepts the deployed Personal SnapTrade auth-mode label without commercial credentials', () => {
    const configuration = { ...configurations[3], authMode: 'personal-oauth', userId: undefined }
    const secrets = { ...providerSecrets, 'connection-4': { consumerKey: 'key' } }
    const [connection] = parseRuntimeConnections({
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([configuration]),
      FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify(secrets),
    }, 'dep-1')
    expect(connection).toMatchObject({ provider: 'snaptrade', authMode: 'personal' })
    expect(connection).not.toHaveProperty('userId')
    expect(connection).not.toHaveProperty('userSecret')
  })

  it('uses current Personal SnapTrade account and unified-position endpoints', async () => {
    const originalFetch = globalThis.fetch
    const requests: string[] = []
    const requestUrls: URL[] = []
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(input instanceof Request ? input.url : input)
      requests.push(url.pathname)
      requestUrls.push(url)
      if (url.pathname === '/api/v1/accounts') {
        return new Response(JSON.stringify([{
          id: 'account-1', name: 'Brokerage', number: '1234', institution_name: 'Example Broker', raw_type: 'Margin',
          balance: { total: { amount: 100, currency: 'USD' } },
        }]))
      }
      if (url.pathname === '/api/v1/accounts/account-1/positions/all') {
        return new Response(JSON.stringify({
          results: [{
            instrument: { kind: 'etf', id: 'asset-1', symbol: 'VTI', description: 'Vanguard Total Stock ETF', currency: 'USD' },
            units: '2', price: '50', cost_basis: '40', currency: 'USD',
          }],
          data_freshness: { as_of: '2026-08-05T00:00:00Z' },
        }))
      }
      if (url.pathname === '/api/v1/accounts/account-1/activities') {
        const offset = url.searchParams.get('offset')
        return new Response(JSON.stringify(offset === '0'
          ? { data: [{ type: 'BUY' }], pagination: { offset: 0, limit: 1, total: 2 } }
          : { data: [{ type: 'SELL' }], pagination: { offset: 1, limit: 1, total: 2 } }))
      }
      throw new Error(`unexpected SnapTrade path ${url.pathname}`)
    }) as typeof fetch
    try {
      const [connection] = parseRuntimeConnections(runtimeEnv([{
        ...configurations[3], authMode: 'personal-oauth', userId: undefined,
      }]), 'dep-1')
      if (connection.provider !== 'snaptrade') throw new Error('missing SnapTrade fixture')
      const accounts = await createProviderTransportFactories().snaptrade(connection).fetchAccounts()
      expect(requests).toEqual(['/api/v1/accounts', '/api/v1/accounts/account-1/positions/all'])
      expect(accounts).toEqual([expect.objectContaining({
        total_value: { value: 100, currency: 'USD' },
        positions: [expect.objectContaining({
          symbol: expect.objectContaining({ id: 'asset-1', symbol: 'VTI' }),
          market_value: 100,
        })],
      })])
      const activities = await createProviderTransportFactories().snaptrade(connection).fetchActivities('2026-01-01', '2026-08-05', ['account-1'])
      expect(requests).toContain('/api/v1/accounts/account-1/activities')
      expect(requestUrls.filter(url => url.pathname === '/api/v1/accounts/account-1/activities')).toHaveLength(2)
      const activityUrls = requestUrls.filter(url => url.pathname === '/api/v1/accounts/account-1/activities')
      expect(activityUrls[0]?.searchParams.get('offset')).toBe('0')
      expect(activityUrls[0]?.searchParams.get('limit')).toBe('1000')
      expect(activityUrls[1]?.searchParams.get('offset')).toBe('1')
      expect(activities).toEqual([
        { account: { id: 'account-1' }, type: 'BUY' },
        { account: { id: 'account-1' }, type: 'SELL' },
      ])
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('rejects an empty SnapTrade page before the reported total', async () => {
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => new Response(JSON.stringify({
      data: [], pagination: { offset: 0, limit: 1000, total: 1 },
    })) ) as typeof fetch
    try {
      const [connection] = parseRuntimeConnections(runtimeEnv([{
        ...configurations[3], authMode: 'personal-oauth', userId: undefined,
      }]), 'dep-1')
      if (connection.provider !== 'snaptrade') throw new Error('missing SnapTrade fixture')
      await expect(createProviderTransportFactories().snaptrade(connection)
        .fetchActivities('2026-01-01', '2026-08-05', ['account-1'])).rejects
        .toThrow('ended before the reported total')
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('rejects SnapTrade activity rows with the wrong account provenance', async () => {
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => new Response(JSON.stringify({
      data: [{ id: 'activity-1', account: { id: 'other-account' } }],
      pagination: { offset: 0, limit: 1000, total: 1 },
    })) ) as typeof fetch
    try {
      const [connection] = parseRuntimeConnections(runtimeEnv([{
        ...configurations[3], authMode: 'personal-oauth', userId: undefined,
      }]), 'dep-1')
      if (connection.provider !== 'snaptrade') throw new Error('missing SnapTrade fixture')
      await expect(createProviderTransportFactories().snaptrade(connection)
        .fetchActivities('2026-01-01', '2026-08-05', ['account-1'])).rejects
        .toThrow('provenance mismatch')
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('rejects secret material in metadata, duplicate ids, and incomplete mappings', () => {
    expect(() => parseRuntimeConnections({ FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([{ ...configurations[0], accessToken: 'forbidden' }]) }, 'dep-1')).toThrow('secret material')
    expect(() => parseRuntimeConnections(runtimeEnv([configurations[0], configurations[0]]), 'dep-1')).toThrow('unique')
    expect(() => parseRuntimeConnections(runtimeEnv([{ ...configurations[0], wealthfolioAccountIds: {} }]), 'dep-1')).toThrow('every provider account')
  })

  it('admits the documented Investec and Mercury sandbox origins without broadening host trust', () => {
    const parsed = parseRuntimeConnections(runtimeEnv(), 'dep-1')
    const investec = parsed.find(connection => connection.provider === 'investec')
    const mercury = parsed.find(connection => connection.provider === 'mercury')
    if (!investec || investec.provider !== 'investec' || !mercury || mercury.provider !== 'mercury') throw new Error('missing provider fixtures')
    expect(() => createProviderTransportFactories({
      FINANCE_SYNC_INVESTEC_URL: 'https://openapisandbox.investec.com',
    }).investec(investec)).not.toThrow()
    expect(() => createProviderTransportFactories({
      FINANCE_SYNC_MERCURY_URL: 'https://api-sandbox.mercury.com',
    }).mercury(mercury)).not.toThrow()
    expect(() => createProviderTransportFactories({
      FINANCE_SYNC_MERCURY_URL: 'https://api-sandbox.mercury.com.attacker.invalid',
    }).mercury(mercury)).toThrow('host is not allowed')
  })

  it('normalizes Mercury accounts to provider-native USD when the API omits currency', async () => {
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => new Response(JSON.stringify({ accounts: [{
      id: 'account-1', name: 'Operating', type: 'checking', status: 'active',
      currentBalance: 1, availableBalance: 1,
    }] }))) as typeof fetch
    try {
      const mercury = parseRuntimeConnections(runtimeEnv(), 'dep-1').find(connection => connection.provider === 'mercury')
      if (!mercury || mercury.provider !== 'mercury') throw new Error('missing Mercury fixture')
      const page = await createProviderTransportFactories({ FINANCE_SYNC_MERCURY_URL: 'https://api.mercury.com/api/v1' }).mercury(mercury).fetchAccounts()
      expect(page.accounts).toEqual([expect.objectContaining({ id: 'account-1', currency: 'USD' })])
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('uses both Plaid transactions and liabilities runtime requests', async () => {
    const requests: string[] = []
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: string | URL | Request) => {
      const path = new URL(input instanceof Request ? input.url : input).pathname
      requests.push(path)
      if (path === '/transactions/sync') return new Response(JSON.stringify({ added: [], modified: [], removed: [], next_cursor: 'next', has_more: false }))
      if (path === '/liabilities/get') return new Response(JSON.stringify({ liabilities: { credit: [] } }))
      throw new Error(`unexpected path ${path}`)
    }) as typeof fetch
    try {
      const [connection] = parseRuntimeConnections(runtimeEnv([configurations[0]]), 'dep-1')
      if (connection.provider !== 'plaid') throw new Error('expected Plaid fixture')
      const transport = createProviderTransportFactories({ FINANCE_SYNC_PLAID_URL: 'https://sandbox.plaid.com' }).plaid(connection)
      await transport.syncTransactions(connection.accessToken)
      await transport.getLiabilities?.(connection.accessToken)
      expect(requests).toEqual(['/transactions/sync', '/liabilities/get'])
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('keeps a documented Plaid Item error code and drops unknown codes and provider text', async () => {
    const originalFetch = globalThis.fetch
    const failWith = (body: Record<string, unknown>) => {
      globalThis.fetch = (async () => new Response(JSON.stringify(body), { status: 400 })) as unknown as typeof fetch
    }
    try {
      const [connection] = parseRuntimeConnections(runtimeEnv([configurations[0]]), 'dep-1')
      if (connection.provider !== 'plaid') throw new Error('expected Plaid fixture')
      const transport = createProviderTransportFactories({ FINANCE_SYNC_PLAID_URL: 'https://sandbox.plaid.com' }).plaid(connection)

      // Outside the earlier hand-picked allowlist, so production logged only the error type.
      for (const code of ['NO_ACCOUNTS', 'ACCESS_NOT_GRANTED', 'NO_LIABILITY_ACCOUNTS', 'PRODUCTS_NOT_SUPPORTED']) {
        failWith({ error_type: 'ITEM_ERROR', error_code: code, error_message: 'provider text account-1 server-token', request_id: 'request-1' })
        const failure = await transport.syncTransactions(connection.accessToken).catch((error: unknown) => error) as Error & { code?: string; errorType?: string }
        expect(failure.code).toBe(code)
        expect(failure.errorType).toBe('ITEM_ERROR')
        expect(failure.message).toBe('plaid request failed: HTTP 400')
      }

      failWith({ error_type: 'ITEM_ERROR', error_code: 'account-1 server-token', error_message: 'provider text' })
      const unknown = await transport.syncTransactions(connection.accessToken).catch((error: unknown) => error) as Error & { code?: string; errorType?: string }
      expect(unknown.code).toBeUndefined()
      expect(unknown.errorType).toBe('ITEM_ERROR')
      expect(JSON.stringify({ ...unknown, message: unknown.message })).not.toMatch(/account-1|server-token|provider text/)
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('advances the Investec transaction upper bound to the poll date', async () => {
    const connection = parseRuntimeConnections(runtimeEnv(), 'dep-1').find(item => item.provider === 'investec')
    if (!connection || connection.provider !== 'investec') throw new Error('missing Investec fixture')
    let requestedWindow: { fromDate: string; toDate: string } | undefined
    const injected = factories()
    injected.investec = () => ({
      listAccounts: async () => [{ accountId: 'account-1', productName: 'Private Bank' }],
      getBalance: async () => ({ currency: 'ZAR', currentBalance: 1 }),
      getTransactionPage: async (_accountId, window) => { requestedWindow = window; return { transactions: [] } },
    })
    await runRuntimeProvider(connection, null, injected)
    expect(requestedWindow).toEqual({
      fromDate: '2026-01-01',
      toDate: new Date().toISOString().slice(0, 10),
    })
  })

  it('dispatches every provider through injected read-only transports with bounded restart cursors', async () => {
    const parsed = parseRuntimeConnections(runtimeEnv(), 'dep-1')
    const results = await Promise.all(parsed.map(connection => runRuntimeProvider(connection, null, factories())))
    expect(results[0].nextCursor).toBe('plaid-next')
    expect(results[1].nextCursor).toBeNull()
    expect(results[2].nextCursor).toBeNull()
    expect(results[3].nextCursor).toBe('2026-07-18')
    expect(results[1].observations).toHaveLength(1)
    expect(results[1].observations[0]).toMatchObject({ domain: 'balance', providerVersion: 'investec:balance:account-1' })
    expect(results[2].observations[0].payload).toMatchObject({ accountId: 'account-1', currency: 'USD' })
    expect(results[3].observations[0].payload).toMatchObject({ accountId: 'account-1', currency: 'USD' })
  })
})
