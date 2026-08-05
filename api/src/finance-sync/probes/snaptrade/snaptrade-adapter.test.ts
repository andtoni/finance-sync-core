import { describe, expect, it, vi } from 'vitest'
import {
  snaptradeResourceKind,
  normalizeAccount,
  normalizeHoldings,
  normalizeActivities,
  runSnapTradeSync,
  type SnapTradeTransport,
  type SnapTradeSyncConfig,
  type SnapTradeRawAccount,
  type SnapTradeRawActivity,
} from './snaptrade-adapter'

// ─── Fixtures ───────────────────────────────────────────────────────────────────

const ACCOUNT_FIXTURE: SnapTradeRawAccount = {
  account: {
    id: 'acc-1',
    name: 'Taxable Brokerage',
    number: '1234',
    institution_name: 'Fidelity',
    raw_type: 'taxable',
    meta: { type: 'individual' },
  },
  name: 'Taxable Brokerage',
  positions: [
    {
      symbol: {
        symbol: { symbol: 'VTI' },
        ticker: 'VTI',
        name: 'Vanguard Total Stock Market ETF',
        currency: { code: 'USD' },
        type: { code: 'ETF', description: 'Exchange Traded Fund' },
      },
      units: 100,
      price: 278.45,
      market_value: '27845.00',
      cost_basis: '25000.00',
    },
    {
      symbol: {
        ticker: 'CASH',
        name: 'Cash/Money Market',
        currency: { code: 'USD' },
        type: { code: 'CASH', description: 'Cash' },
      },
      units: 500.12,
      price: 1.00,
      market_value: '500.12',
    },
  ],
  total_value: { value: '28345.12', currency: 'USD' },
}

const ACCOUNT_FIXTURE_2: SnapTradeRawAccount = {
  account: {
    id: 'acc-2',
    name: 'Roth IRA',
    number: '5678',
    institution_name: 'Schwab',
    raw_type: 'roth',
  },
  positions: [
    {
      symbol: {
        symbol: { symbol: 'SCHB' },
        ticker: 'SCHB',
        name: 'Schwab U.S. Broad Market ETF',
        currency: { code: 'USD' },
        type: { code: 'ETF', description: 'ETF' },
      },
      units: 50,
      price: 65.30,
      market_value: '3265.00',
    },
  ],
  total_value: { value: '3265.00', currency: 'USD' },
}

const ACTIVITIES_FIXTURE: readonly SnapTradeRawActivity[] = [
  {
    id: 'act-1',
    account: { id: 'acc-1' },
    type: 'DIVIDEND',
    symbol: { symbol: { symbol: 'VTI' }, ticker: 'VTI' },
    units: null,
    price: null,
    amount: 118.40,
    currency: { code: 'USD' },
    trade_date: '2026-06-01T00:00:00Z',
    description: 'VTI dividend',
  },
  {
    id: 'act-2',
    account: { id: 'acc-1' },
    type: 'BUY',
    symbol: { symbol: 'VXUS', ticker: 'VXUS' },
    units: 10,
    price: 64.20,
    amount: null,
    currency: { code: 'USD' },
    trade_date: '2026-06-05T00:00:00Z',
  },
  {
    id: 'act-3',
    account: { id: 'acc-2' },
    type: 'CONTRIBUTION',
    symbol: null,
    amount: '500.00',
    currency: { code: 'USD' },
    trade_date: '2026-06-10T00:00:00Z',
  },
]

// Mock transport factory
function mockTransport(
  accounts: readonly SnapTradeRawAccount[] = [ACCOUNT_FIXTURE, ACCOUNT_FIXTURE_2],
  activities: readonly SnapTradeRawActivity[] = ACTIVITIES_FIXTURE,
): SnapTradeTransport {
  return {
    fetchAccounts: vi.fn().mockResolvedValue(accounts),
    fetchActivities: vi.fn().mockResolvedValue(activities),
  }
}

const DEFAULT_CONFIG: SnapTradeSyncConfig = {
  deploymentId: 'g7-test',
  localOwnerId: 'alexis',
  sourceResourceIncarnationId: 'snaptrade:user-1',
  activitiesStartDate: '2026-01-01',
  activitiesEndDate: '2026-07-11',
}

// ─── Tests: domain mapping ──────────────────────────────────────────────────────

describe('snaptradeResourceKind', () => {
  it('maps SnapTrade resource kinds to canonical domains', () => {
    expect(snaptradeResourceKind({ domain: 'brokerage_account' })).toBe('balance')
    expect(snaptradeResourceKind({ domain: 'brokerage_cash' })).toBe('cash_ledger')
    expect(snaptradeResourceKind({ domain: 'holding' })).toBe('holding')
    expect(snaptradeResourceKind({ domain: 'brokerage_activity' })).toBe('brokerage_activity')
    expect(snaptradeResourceKind({ domain: 'security' })).toBe('security')
  })
})

// ─── Tests: normalizeAccount ────────────────────────────────────────────────────

describe('normalizeAccount', () => {
  it('extracts account metadata and total value', () => {
    const n = normalizeAccount(ACCOUNT_FIXTURE)
    expect(n.id).toBe('acc-1')
    expect(n.name).toBe('Taxable Brokerage')
    expect(n.institutionName).toBe('Fidelity')
    expect(n.totalValue).toBe('28345.12')
    expect(n.currency).toBe('USD')
    expect(n.brokerTag).toBe('fidelity')
    expect(n.accountTypeTag).toBe('taxable')
  })

  it('handles missing details gracefully', () => {
    const minimal: SnapTradeRawAccount = { name: 'Minimal' }
    const n = normalizeAccount(minimal)
    expect(n.id).toBe('Minimal')
    expect(n.name).toBe('Minimal')
    expect(n.institutionName).toBeNull()
    expect(n.totalValue).toBeNull()
    expect(n.accountTypeTag).toBeNull()
  })

  it('falls back through id hierarchy', () => {
    const byNumber: SnapTradeRawAccount = { account: { number: '9999' } }
    expect(normalizeAccount(byNumber).id).toBe('9999')
  })
})

// ─── Tests: normalizeHoldings ───────────────────────────────────────────────────

describe('normalizeHoldings', () => {
  it('extracts positions from an account', () => {
    const holdings = normalizeHoldings(ACCOUNT_FIXTURE)
    expect(holdings.length).toBe(2)

    const vti = holdings[0]
    expect(vti.symbol).toBe('VTI')
    expect(vti.units).toBe('100.0000')
    expect(vti.price).toBe('278.45')
    expect(vti.value).toBe('27845.00')
    expect(vti.currency).toBe('USD')
    expect(vti.costBasis).toBe('25000.00')
    expect(vti.instrumentTypeTag).toBe('etf')
    expect(vti.brokerTag).toBe('fidelity')
    expect(vti.accountTypeTag).toBe('taxable')

    const cash = holdings[1]
    expect(cash.symbol).toBe('CASH')
    expect(cash.value).toBe('500.12')
    expect(cash.instrumentTypeTag).toBe('cash')
  })

  it('handles empty positions array', () => {
    const empty: SnapTradeRawAccount = { account: { id: 'acc-empty' }, positions: [] }
    expect(normalizeHoldings(empty)).toEqual([])
  })

  it('handles missing symbol gracefully', () => {
    const noSym: SnapTradeRawAccount = {
      account: { id: 'acc-nosym' },
      positions: [{ units: 10, price: 50 }],
    }
    const holdings = normalizeHoldings(noSym)
    expect(holdings.length).toBe(1)
    expect(holdings[0].symbol).toBe('?')
    expect(holdings[0].instrumentTypeTag).toBe('cash')
  })
})

// ─── Tests: normalizeActivities ─────────────────────────────────────────────────

describe('normalizeActivities', () => {
  it('maps supported activity types with ids, skips unmappable rows', () => {
    const payload: readonly SnapTradeRawActivity[] = [
      ...ACTIVITIES_FIXTURE,
      // Unmappable rows that should be skipped
      { id: 'skip-1', type: 'REI', amount: '12.00', trade_date: '2026-06-16' },
      { type: 'DIVIDEND', amount: '9.99', trade_date: '2026-06-17' },
      { id: 'skip-3', type: 'DIVIDEND', amount: '9.99' },
      { id: 'skip-4', type: 'BUY', units: 3, amount: null, trade_date: '2026-06-18' },
    ]
    const result = normalizeActivities(payload)
    expect(result.map(r => r.id)).toEqual([
      'snaptrade:act-1',
      'snaptrade:act-2',
      'snaptrade:act-3',
    ])
    expect(result.map(r => r.type)).toEqual(['dividend', 'buy', 'contribution'])
    expect(result[0]?.providerAssetId).toBeNull()
  })

  it('accepts the paginated { data: [...] } wrapper', () => {
    expect(normalizeActivities({ data: ACTIVITIES_FIXTURE }).length).toBe(3)
  })

  it('rejects garbage payloads gracefully', () => {
    expect(normalizeActivities(null)).toEqual([])
    expect(normalizeActivities({ nope: 1 })).toEqual([])
    expect(normalizeActivities('oops')).toEqual([])
  })

  it('derives amount via broker money and falls back to derived', () => {
    const payload: readonly SnapTradeRawActivity[] = [
      {
        id: 'derived-1',
        account: { id: 'acc-1' },
        type: 'BUY',
        symbol: { symbol: 'VTI', ticker: 'VTI' },
        units: 10,
        price: 278.45,
        amount: null,
        currency: { code: 'USD' },
        trade_date: '2026-06-05T00:00:00Z',
      },
    ]
    const result = normalizeActivities(payload)
    expect(result.length).toBe(1)
    expect(result[0].amount).toBe('2784.50') // 10 × 278.45
  })
})

// ─── Tests: runSnapTradeSync (integration) ──────────────────────────────────────

describe('runSnapTradeSync', () => {
  it('syncs accounts and holdings, produces observations with semantic hashes', async () => {
    const transport = mockTransport()
    const result = await runSnapTradeSync(DEFAULT_CONFIG, transport)

    expect(transport.fetchAccounts).toHaveBeenCalledOnce()
    expect(transport.fetchActivities).toHaveBeenCalledWith('2026-01-01', '2026-07-11', ['acc-1', 'acc-2'])

    expect(result.accountsFetched).toBe(2)
    expect(result.holdingsObserved).toBe(3) // VTI + CASH in acc-1, SCHB in acc-2
    expect(result.activitiesObserved).toBe(3)

    expect(result.accounts.length).toBe(2)
    expect(result.accounts[0].id).toBe('acc-1')
    expect(result.accounts[1].id).toBe('acc-2')

    expect(result.holdings.length).toBe(3)
    expect(result.holdings[0].symbol).toBe('VTI')
    expect(result.holdings[1].symbol).toBe('CASH')
    expect(result.holdings[2].symbol).toBe('SCHB')

    expect(result.activities.length).toBe(3)
    expect(result.activities[0].type).toBe('dividend')
    expect(result.activities[1].type).toBe('buy')
    expect(result.activities[2].type).toBe('contribution')

    // Observations: 2 accounts + 3 holdings + 3 activities = 8
    expect(result.observations.length).toBe(8)

    // Verify semantic hash shape
    for (const obs of result.observations) {
      expect(obs.semanticHash).toMatch(/^[a-f0-9]{64}$/)
      expect(obs.deploymentId).toBe('g7-test')
      expect(obs.localOwnerId).toBe('alexis')
      expect(obs.hashSpecificationVersion).toBe(1)
    }

    // Verify domain assignments
    const domains = result.observations.map(o => o.domain)
    expect(domains.filter(d => d === 'balance').length).toBe(2)
    expect(domains.filter(d => d === 'holding').length).toBe(2) // VTI and SCHB
    expect(domains.filter(d => d === 'cash_ledger').length).toBe(1) // CASH
    expect(domains.filter(d => d === 'brokerage_activity').length).toBe(3)
  })

  it('quarantines unmapped synthetic assets and permits only explicit immutable mappings', async () => {
    const result = await runSnapTradeSync({ ...DEFAULT_CONFIG, assetMappings: { 'asset-vti': 'wf-vti' } }, mockTransport([{
      ...ACCOUNT_FIXTURE,
      positions: [{ ...ACCOUNT_FIXTURE.positions![0]!, symbol: { ...ACCOUNT_FIXTURE.positions![0]!.symbol!, id: 'asset-vti' } },
        { ...ACCOUNT_FIXTURE.positions![1]!, symbol: { ...ACCOUNT_FIXTURE.positions![1]!.symbol!, id: 'asset-cash' } }],
    }], []))
    expect(result.complete).toBe(false)
    expect(result.quarantinedAssetIds).toEqual(['asset-cash'])
    expect(result.holdings.map(holding => holding.providerAssetId)).toEqual(['asset-vti'])
    expect(result.observations.find(observation => observation.payload.providerAssetId === 'asset-vti')?.payload).toMatchObject({ wealthfolioAssetId: 'wf-vti' })
  })

  it('preserves activity asset IDs and quarantines unmapped activity observations', async () => {
    const activities = [{ ...ACTIVITIES_FIXTURE[0]!, symbol: { ...ACTIVITIES_FIXTURE[0]!.symbol!, id: 'asset-vti' } }]
    const result = await runSnapTradeSync({ ...DEFAULT_CONFIG, assetMappings: { 'asset-holding': 'wf-holding' } }, mockTransport([{
      ...ACCOUNT_FIXTURE, positions: [{ ...ACCOUNT_FIXTURE.positions![0]!, symbol: { ...ACCOUNT_FIXTURE.positions![0]!.symbol!, id: 'asset-holding' } }],
    }], activities))
    expect(result.activitiesObserved).toBe(0)
    expect(result.complete).toBe(false)
    expect(result.quarantinedAssetIds).toContain('asset-vti')
    expect(result.observations.some(observation => observation.providerVersion.includes('activity:'))).toBe(false)
  })

  it('filters to configured account IDs', async () => {
    const config: SnapTradeSyncConfig = {
      ...DEFAULT_CONFIG,
      accountIds: ['acc-1'],
    }
    const transport = mockTransport()
    const result = await runSnapTradeSync(config, transport)

    expect(result.accounts.length).toBe(1)
    expect(result.accounts[0].id).toBe('acc-1')
    expect(result.holdings.length).toBe(2)
  })

  it('returns empty result on fetchAccounts failure', async () => {
    const transport: SnapTradeTransport = {
      fetchAccounts: vi.fn().mockRejectedValue(new Error('network error')),
      fetchActivities: vi.fn(),
    }
    const result = await runSnapTradeSync(DEFAULT_CONFIG, transport)
    expect(result.accountsFetched).toBe(0)
    expect(result.holdingsObserved).toBe(0)
    expect(result.activitiesObserved).toBe(0)
    expect(result.observations.length).toBe(0)
  })

  it('tolerates activities fetch failure, still returns accounts and holdings', async () => {
    const transport: SnapTradeTransport = {
      fetchAccounts: vi.fn().mockResolvedValue([ACCOUNT_FIXTURE]),
      fetchActivities: vi.fn().mockRejectedValue(new Error('activities down')),
    }
    const result = await runSnapTradeSync(DEFAULT_CONFIG, transport)
    expect(result.accountsFetched).toBe(1)
    expect(result.holdingsObserved).toBe(2)
    expect(result.activitiesObserved).toBe(0)
    // Should have balance (1) + holdings (2) = 3 observations
    expect(result.observations.length).toBe(3)
  })

  it('handles empty account list', async () => {
    const transport = mockTransport([])
    const result = await runSnapTradeSync(DEFAULT_CONFIG, transport)
    expect(result.accountsFetched).toBe(0)
    expect(result.observations.length).toBe(0)
  })

  it('handles empty activities gracefully', async () => {
    const transport = mockTransport([ACCOUNT_FIXTURE], [])
    const result = await runSnapTradeSync(DEFAULT_CONFIG, transport)
    expect(result.activitiesObserved).toBe(0)
    // Should still have account + holding observations
    expect(result.observations.length).toBe(3) // 1 balance + 2 holdings
  })

  it('uses default date range when not configured', async () => {
    const transport = mockTransport([ACCOUNT_FIXTURE], [])
    await runSnapTradeSync({
      deploymentId: 'g7-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'snaptrade:user-1',
    }, transport)

    expect(transport.fetchActivities).toHaveBeenCalled()
    const callArgs = (transport.fetchActivities as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(callArgs[0]).toMatch(/^\d{4}-\d{2}-\d{2}$/) // startDate
    expect(callArgs[1]).toMatch(/^\d{4}-\d{2}-\d{2}$/) // endDate
  })

  it('produces stable semantic hashes for identical inputs', async () => {
    const transport1 = mockTransport([ACCOUNT_FIXTURE], ACTIVITIES_FIXTURE)
    const transport2 = mockTransport([ACCOUNT_FIXTURE], ACTIVITIES_FIXTURE)

    const result1 = await runSnapTradeSync(DEFAULT_CONFIG, transport1)
    const result2 = await runSnapTradeSync(DEFAULT_CONFIG, transport2)

    expect(result1.observations.length).toBe(result2.observations.length)
    for (let i = 0; i < result1.observations.length; i++) {
      expect(result1.observations[i].semanticHash).toBe(result2.observations[i].semanticHash)
      expect(result1.observations[i].providerVersion).toBe(result2.observations[i].providerVersion)
    }
  })

  it('never exposes secret values in observation payloads', async () => {
    const transport = mockTransport()
    const result = await runSnapTradeSync(DEFAULT_CONFIG, transport)
    const serialized = JSON.stringify(result)
    expect(serialized).not.toContain('secret')
    expect(serialized).not.toContain('consumer-key')
    expect(serialized).not.toContain('client-id')
  })
})
