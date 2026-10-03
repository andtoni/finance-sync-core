import { describe, expect, it } from 'vitest'
import { runMercurySync, mercuryResourceKind } from './mercury-adapter'
import type { MercuryTransport, MercuryAccount, MercuryTransaction } from './mercury-adapter'

function mockAccount(overrides: Partial<MercuryAccount> = {}): MercuryAccount {
  return {
    id: 'acc-1',
    name: 'Business Checking',
    type: 'cash',
    currentBalance: 15000.00,
    availableBalance: 14250.00,
    currency: 'USD',
    status: 'active',
    ...overrides,
  }
}

function mockTransaction(overrides: Partial<MercuryTransaction> = {}): MercuryTransaction {
  return {
    id: 'txn-1',
    accountId: 'acc-1',
    amount: -250.00,
    counterpartyName: 'AWS AMAZON',
    bankDescription: null,
    note: null,
    createdAt: '2026-07-10T14:30:00.000Z',
    postedAt: '2026-07-10T14:30:00.000Z',
    status: 'sent',
    currency: 'USD',
    ...overrides,
  }
}

function mockTransport(overrides: Partial<MercuryTransport> = {}): MercuryTransport {
  const base: MercuryTransport = {
    fetchAccounts: async () => ({
      accounts: [mockAccount()],
    }),
    fetchTransactions: async (_accountId, _offset, _limit, _since) => ({
      transactions: [mockTransaction()],
      total: 1,
    }),
  }
  return { ...base, ...overrides }
}

describe('Mercury provider adapter G6', () => {
  it('maps Mercury account types to canonical domains', () => {
    expect(mercuryResourceKind('cash')).toBe('cash_ledger')
    expect(mercuryResourceKind('depository')).toBe('cash_ledger')
    expect(mercuryResourceKind('credit')).toBe('liability')
    expect(mercuryResourceKind('loan')).toBe('liability')
    expect(mercuryResourceKind('investment')).toBe('brokerage_activity')
    expect(mercuryResourceKind('other')).toBe('cash_ledger')
  })

  it('fetches accounts and produces balance + transaction observations', async () => {
    const transport = mockTransport()
    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(result.accountsFetched).toBe(1)
    expect(result.accounts.length).toBe(1)
    expect(result.accounts[0].id).toBe('acc-1')
    expect(result.accounts[0].name).toBe('Business Checking')

    expect(result.observations.length).toBe(2)

    const balanceObs = result.observations.find(o => o.providerVersion === 'mercury:balance:acc-1')
    expect(balanceObs).toBeDefined()
    expect(balanceObs!.domain).toBe('cash_ledger')
    expect(balanceObs!.payload).toMatchObject({
      accountId: 'acc-1',
      currentBalance: '15000.00',
      availableBalance: '14250.00',
      currency: 'USD',
    })
    expect(balanceObs!.semanticHash).toMatch(/^[a-f0-9]{64}$/)

    const txnObs = result.observations.find(o => o.providerVersion === 'mercury:txn:txn-1')
    expect(txnObs).toBeDefined()
    expect(txnObs!.domain).toBe('cash_ledger')
    expect(txnObs!.payload).toMatchObject({
      transactionId: 'txn-1',
      accountId: 'acc-1',
      amount: '-250.00',
      currency: 'USD',
      status: 'posted',
    })
    expect(txnObs!.semanticHash).toMatch(/^[a-f0-9]{64}$/)
  })

  it('filters to configured account IDs only', async () => {
    const transport = mockTransport({
      fetchAccounts: async () => ({
        accounts: [
          mockAccount({ id: 'acc-1', name: 'Checking' }),
          mockAccount({ id: 'acc-2', name: 'Savings' }),
        ],
      }),
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
      accountIds: ['acc-1'],
    }, transport)

    expect(result.accountsFetched).toBe(2)
    expect(result.accounts.length).toBe(1)
    expect(result.accounts[0].id).toBe('acc-1')
  })

  it('emits balance observations for all accounts but polls txn only for active ones', async () => {
    const transport = mockTransport({
      fetchAccounts: async () => ({
        accounts: [
          mockAccount({ id: 'acc-1', status: 'active' }),
          mockAccount({ id: 'acc-2', status: 'closed' }),
        ],
      }),
      fetchTransactions: async (accountId) => {
        if (accountId === 'acc-2') throw new Error('should not be called')
        return { transactions: [mockTransaction()], total: 1 }
      },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    // acc-1: balance obs + txn obs
    // acc-2: balance obs (no txn polling because closed)
    expect(result.observations.length).toBe(3)
    expect(result.accounts.length).toBe(1)
    expect(result.accounts[0].id).toBe('acc-1')

    const balanceIds = result.observations
      .filter(o => o.providerVersion.startsWith('mercury:balance:'))
      .map(o => o.payload.accountId)
    expect(balanceIds).toEqual(['acc-1', 'acc-2'])
  })

  it('retains Mercury pending status for native projection', async () => {
    const result = await runMercurySync({
      deploymentId: 'g6-test', localOwnerId: 'alexis', sourceResourceIncarnationId: 'mercury:org-1',
    }, mockTransport({ fetchTransactions: async () => ({
      transactions: [mockTransaction({ id: 'pending-1', status: 'pending', postedAt: null })], total: 1,
    }) }))
    expect(result.observations.find(o => o.providerVersion === 'mercury:txn:pending-1')?.payload).toMatchObject({ status: 'pending' })
  })

  it('quarantines sent/posted/pending transactions without provider timestamps', async () => {
    const result = await runMercurySync({ deploymentId: 'g6-test', localOwnerId: 'alexis', sourceResourceIncarnationId: 'mercury:org-1' }, mockTransport({
      fetchTransactions: async () => ({ transactions: [
        mockTransaction({ id: 'sent-no-date', status: 'sent', postedAt: null, createdAt: null }),
        mockTransaction({ id: 'posted-no-date', status: 'posted', postedAt: null, createdAt: null }),
        mockTransaction({ id: 'pending-no-date', status: 'pending', postedAt: null, createdAt: null }),
        mockTransaction({ id: 'posted-invalid-date', status: 'posted', postedAt: 'not-a-date' }),
      ], total: 4 }),
    }))
    expect(result.observations.some(item => item.providerVersion.includes('no-date'))).toBe(false)
    expect(result.quarantinedTransactions).toBe(4)
    expect(result.paginationComplete).toBe(false)
  })

  it('retracts cancelled and failed transactions without failing the account', async () => {
    const transport = mockTransport({
      fetchTransactions: async () => ({
        transactions: [
          mockTransaction({ id: 'txn-1', status: 'sent', amount: -50 }),
          mockTransaction({ id: 'txn-2', status: 'cancelled', amount: -100 }),
          mockTransaction({ id: 'txn-3', status: 'failed', amount: -200 }),
          mockTransaction({ id: 'txn-4', status: 'pending', amount: -75 }),
        ],
        total: 4,
      }),
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(result.transactionsObserved).toBe(4)
    expect(result.quarantinedTransactions).toBe(0)
    expect(result.paginationComplete).toBe(true)
    const transactions = result.observations.filter(o => o.providerVersion.startsWith('mercury:txn:'))
    expect(transactions.map(o => [o.payload.transactionId, o.payload.isRemoved === true])).toEqual([
      ['txn-1', false], ['txn-2', true], ['txn-3', true], ['txn-4', false],
    ])
    // A tombstone carries only the identity needed to retract the activity.
    expect(transactions[1].payload).toEqual({ transactionId: 'txn-2', accountId: transactions[0].payload.accountId, isRemoved: true })
  })

  it('still fails closed on a transaction status it does not know', async () => {
    const result = await runMercurySync({ deploymentId: 'g6-test', localOwnerId: 'alexis', sourceResourceIncarnationId: 'mercury:org-1' }, mockTransport({
      fetchTransactions: async () => ({ transactions: [mockTransaction({ id: 'txn-9', status: 'reversed' })], total: 1 }),
    }))
    expect(result.quarantinedTransactions).toBe(1)
    expect(result.paginationComplete).toBe(false)
    expect(result.observations.some(o => o.providerVersion === 'mercury:txn:txn-9')).toBe(false)
  })

  it('handles pagination across multiple pages', async () => {
    let callCount = 0
    const transport = mockTransport({
      fetchTransactions: async (_accountId, offset) => {
        callCount++
        if (offset === 0) {
          const items = Array.from({ length: 500 }, (_, i) =>
            mockTransaction({ id: `txn-p1-${i}`, amount: -(i + 1) })
          )
          return { transactions: items, total: 600 }
        }
        const items = Array.from({ length: 100 }, (_, i) =>
          mockTransaction({ id: `txn-p2-${i}`, amount: -(501 + i) })
        )
        return { transactions: items, total: 600 }
      },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(callCount).toBe(2)
    expect(result.pagesFetched).toBe(2)
    expect(result.transactionsObserved).toBe(600)
    expect(result.observations.length).toBe(601)
  })

  it('stops pagination when page is short (last page)', async () => {
    let callCount = 0
    const transport = mockTransport({
      fetchTransactions: async (_accountId, offset) => {
        callCount++
        return {
          transactions: offset === 0
            ? [mockTransaction({ id: 'txn-only', amount: -5 })]
            : [],
          total: 1,
        }
      },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(callCount).toBe(1)
    expect(result.pagesFetched).toBe(1)
    expect(result.transactionsObserved).toBe(1)
  })

  it('handles accounts fetch failure gracefully', async () => {
    const transport = mockTransport({
      fetchAccounts: async () => { throw new Error('API unavailable') },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(result.accounts.length).toBe(0)
    expect(result.observations.length).toBe(0)
    expect(result.accountsFetched).toBe(0)
  })

  it('handles transaction fetch failure mid-drain without crashing', async () => {
    let callCount = 0
    const transport = mockTransport({
      fetchTransactions: async () => {
        callCount++
        if (callCount === 1) {
          return {
            transactions: [mockTransaction({ id: 'txn-before-fail', amount: -100 })],
            total: 5,
          }
        }
        throw new Error('transient failure')
      },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
      maxPagesPerAccount: 10,
    }, transport)

    expect(result.observations.length).toBe(2)
    const txnIds = result.observations
      .filter(o => o.providerVersion.startsWith('mercury:txn:'))
      .map(o => o.payload.transactionId)
    expect(txnIds).toEqual(['txn-before-fail'])
    expect(result.transactionsObserved).toBe(1)
  })

  it('defaults to USD currency when transaction has none', async () => {
    const transport = mockTransport({
      fetchTransactions: async () => ({
        transactions: [
          mockTransaction({ id: 'txn-no-ccy', currency: null }),
        ],
        total: 1,
      }),
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    const txnObs = result.observations.find(o => o.providerVersion === 'mercury:txn:txn-no-ccy')
    expect(txnObs).toBeDefined()
    expect(txnObs!.payload.currency).toBe('USD')
  })

  it('produces deterministic semantic hashes for same data across runs', async () => {
    const transport = mockTransport()
    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    for (const obs of result.observations) {
      expect(obs.semanticHash).toMatch(/^[a-f0-9]{64}$/)
      expect(obs.deploymentId).toBe('g6-test')
      expect(obs.localOwnerId).toBe('alexis')
      expect(obs.hashSpecificationVersion).toBe(2)
    }

    const result2 = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(result.observations.map(o => o.semanticHash))
      .toEqual(result2.observations.map(o => o.semanticHash))
  })

  it('integrates multiple accounts with transactions', async () => {
    const transport = mockTransport({
      fetchAccounts: async () => ({
        accounts: [
          mockAccount({ id: 'acc-checking', name: 'Business Checking' }),
          mockAccount({ id: 'acc-savings', name: 'Business Savings', currentBalance: 50000 }),
        ],
      }),
      fetchTransactions: async (accountId) => {
        if (accountId === 'acc-checking') {
          return {
            transactions: [
              mockTransaction({ id: 'txn-chk-1', amount: -100 }),
              mockTransaction({ id: 'txn-chk-2', amount: -50 }),
            ],
            total: 2,
          }
        }
        return {
          transactions: [
            mockTransaction({ id: 'txn-sav-1', amount: 1000, accountId: 'acc-savings' }),
          ],
          total: 1,
        }
      },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
    }, transport)

    expect(result.observations.length).toBe(5)
    expect(result.accounts.length).toBe(2)
    expect(result.transactionsObserved).toBe(3)
    expect(result.pagesFetched).toBe(2)
  })

  it('respects maxPagesPerAccount cap', async () => {
    let callCount = 0
    const transport = mockTransport({
      fetchTransactions: async (_accountId, _offset) => {
        callCount++
        const txns = Array.from({ length: 500 }, (_, i) =>
          mockTransaction({ id: `txn-${callCount}-${i}`, amount: -i })
        )
        return { transactions: txns, total: 10000 }
      },
    })

    const result = await runMercurySync({
      deploymentId: 'g6-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'mercury:org-1',
      maxPagesPerAccount: 3,
    }, transport)

    expect(callCount).toBe(3)
    expect(result.pagesFetched).toBe(3)
  })
})
