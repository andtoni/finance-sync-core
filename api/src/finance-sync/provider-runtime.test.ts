import { describe, expect, it } from 'vitest'
import { parseRuntimeConnections, runRuntimeProvider, type ProviderTransportFactories } from './provider-runtime'

const common = {
  connectionId: 'connection-1', deploymentId: 'dep-1', localOwnerId: 'urn:test#owner',
  sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
  accountIds: ['account-1'], wealthfolioAccountIds: { 'account-1': 'wf-account-1' }, accessGeneration: 1,
}
const configurations = [
  { ...common, provider: 'plaid' },
  { ...common, connectionId: 'connection-2', provider: 'investec', clientId: 'id', eligibleProductNames: ['Private Bank'], fromDate: '2026-01-01', toDate: '2026-07-18' },
  { ...common, connectionId: 'connection-3', provider: 'mercury', since: '2026-01-01' },
  { ...common, connectionId: 'connection-4', provider: 'snaptrade', clientId: 'id', userId: 'user', activitiesStartDate: '2026-01-01', activitiesEndDate: '2026-07-18' },
]
const providerSecrets = {
  'connection-1': { accessToken: 'server-token' },
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

  it('rejects secret material in metadata, duplicate ids, and incomplete mappings', () => {
    expect(() => parseRuntimeConnections({ FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([{ ...configurations[0], accessToken: 'forbidden' }]) }, 'dep-1')).toThrow('secret material')
    expect(() => parseRuntimeConnections(runtimeEnv([configurations[0], configurations[0]]), 'dep-1')).toThrow('unique')
    expect(() => parseRuntimeConnections(runtimeEnv([{ ...configurations[0], wealthfolioAccountIds: {} }]), 'dep-1')).toThrow('every provider account')
  })

  it('dispatches every provider through injected read-only transports with bounded restart cursors', async () => {
    const parsed = parseRuntimeConnections(runtimeEnv(), 'dep-1')
    const results = await Promise.all(parsed.map(connection => runRuntimeProvider(connection, null, factories())))
    expect(results[0].nextCursor).toBe('plaid-next')
    expect(results[1].nextCursor).toBeNull()
    expect(results[2].nextCursor).toBeNull()
    expect(results[3].nextCursor).toBe('2026-07-18')
    expect(results[1].observations).toEqual([])
    expect(results[2].observations[0].payload).toMatchObject({ accountId: 'account-1', currency: 'USD' })
    expect(results[3].observations[0].payload).toMatchObject({ accountId: 'account-1', currency: 'USD' })
  })
})
