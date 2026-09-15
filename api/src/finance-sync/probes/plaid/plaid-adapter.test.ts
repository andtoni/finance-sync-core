
import { describe, expect, it, vi } from 'vitest'
import { plaidResourceKind, runPlaidSync } from './plaid-adapter'
import type { PlaidTransport } from './plaid-adapter'

function transactionObservations(result: Awaited<ReturnType<typeof runPlaidSync>>) {
  return result.observations.filter(observation => observation.providerVersion.startsWith('plaid:txn:'))
}

function mockTransport(overrides: Partial<PlaidTransport> = {}): PlaidTransport {
  const base: PlaidTransport = {
    syncTransactions: async (_accessToken, cursor) => {
      if (!cursor) {
        return {
          added: [
            { transaction_id: 'txn-1', account_id: 'acc-1', amount: 12.50, date: '2026-07-10', name: 'AMAZON.COM' },
            { transaction_id: 'txn-2', account_id: 'acc-2', amount: -5.00, date: '2026-07-09', name: 'REFUND' },
          ],
          modified: [],
          removed: [],
          accounts: [
            { account_id: 'acc-1', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 1000, iso_currency_code: 'USD' } },
            { account_id: 'acc-2', name: 'Credit Card', type: 'credit', subtype: 'credit card', balances: { current: -200, iso_currency_code: 'USD' } },
          ],
          next_cursor: 'cursor-1',
          has_more: false,
        }
      }
      return { added: [], modified: [], removed: [], accounts: [], next_cursor: cursor, has_more: false }
    },
  }
  return { ...base, ...overrides }
}

describe('Plaid adapter G4', () => {
  it('converts Plaid resource types to canonical domains', () => {
    expect(plaidResourceKind('depository')).toBe('cash_ledger')
    expect(plaidResourceKind('credit')).toBe('liability')
    expect(plaidResourceKind('loan')).toBe('liability')
    expect(plaidResourceKind('brokerage')).toBe('brokerage_activity')
    expect(plaidResourceKind('investment')).toBe('brokerage_activity')
    expect(plaidResourceKind('other')).toBe('cash_ledger')
  })

  it('syncs first page and returns cursor + observations', async () => {
    const transport = mockTransport()
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1', 'acc-2'],
    }, transport)
    expect(result.nextCursor).toBe('cursor-1')
    expect(result.accounts.length).toBe(2)
    const transactions = transactionObservations(result)
    expect(transactions).toHaveLength(2)
    expect(transactions[0].payload).toMatchObject({ transactionId: 'txn-1', accountId: 'acc-1' })
    expect(transactions[1].payload.transactionId).toBe('txn-2')
    expect(result.observations.filter(observation => observation.providerVersion.startsWith('plaid:balance:'))).toHaveLength(2)
    expect(result.observations.filter(observation => observation.providerVersion.startsWith('plaid:liability:'))).toHaveLength(1)
  })

  it('normalizes the real Plaid wire currency when sync pages omit accounts', async () => {
    const transport = mockTransport({
      getAccounts: async () => [{ account_id: 'acc-1', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 100, iso_currency_code: 'USD' } }],
      syncTransactions: async () => ({
        added: [{ transaction_id: 'txn-wire', account_id: 'acc-1', amount: 25, date: '2026-07-10', name: 'SHOP', iso_currency_code: 'USD' }],
        modified: [], removed: [], next_cursor: 'wire-cursor', has_more: false,
      }),
    })
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc', deploymentId: 'g4-test', localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1', accountIds: ['acc-1'],
    }, transport)
    const transactions = transactionObservations(result)
    expect(transactions).toHaveLength(1)
    expect(transactions[0].payload).toMatchObject({ currency: 'USD', accountId: 'acc-1' })
    expect(result.accounts).toHaveLength(1)
  })

  it('handles empty page gracefully', async () => {
    const transport = mockTransport({
      syncTransactions: async () => ({
        added: [], modified: [], removed: [], accounts: [],
        next_cursor: 'cursor-0', has_more: false,
      }),
    })
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: [],
    }, transport)
    expect(result.observations.length).toBe(0)
    expect(result.nextCursor).toBe('cursor-0')
  })

  it('handles mutation restart on MUTATION_DURING_PAGINATION error', async () => {
    let calls = 0
    const transport = mockTransport()
    transport.syncTransactions = async () => {
      calls += 1
      if (calls === 1) return { added: [{ transaction_id: 'txn-1', account_id: 'acc-1', amount: 10, date: '2026-07-10', name: 'TEST' }], modified: [], removed: [], accounts: [], next_cursor: 'cursor-page2', has_more: true }
      if (calls === 2) {
        const err = new Error('mutation') as Error & { code: string }
        err.code = 'TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION'
        throw err
      }
      return { added: [], modified: [], removed: [], accounts: [{ account_id: 'acc-1', name: 'Mock', type: 'depository', subtype: 'checking', balances: { current: 125, iso_currency_code: 'USD' } }], next_cursor: 'cursor-restart', has_more: false }
    }
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-xxx',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
    }, transport)
    expect(result.nextCursor).toBe('cursor-restart')
    expect(calls).toBe(3)
  })

  it('produces stable observation identity from semantic hash', async () => {
    const transport = mockTransport()
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
    }, transport)
    const obs0 = result.observations[0]
    expect(obs0.semanticHash).toMatch(/^[a-f0-9]{64}$/)
    expect(obs0.deploymentId).toBe('g4-test')
    expect(obs0.localOwnerId).toBe('alexis')
  })

  it('refuses an incomplete current-state snapshot with missing account currency', async () => {
    const transport = mockTransport({
      syncTransactions: async () => ({
        added: [{ transaction_id: 'txn-no-ccy', account_id: 'acc-1', amount: 100, date: '2026-07-10', name: 'NOCCY' }],
        modified: [], removed: [],
        accounts: [{ account_id: 'acc-1', name: 'Unknown', type: 'depository', subtype: 'checking', balances: { current: 100, iso_currency_code: null } }],
        next_cursor: 'cursor-x', has_more: false,
      }),
    })
    await expect(runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
    }, transport)).rejects.toThrow('INCOMPLETE_ACCOUNT_BALANCE_CURRENCY')
  })

  it('uses per-account sourceResourceIncarnationIds for checking and credit accounts', async () => {
    const transport = mockTransport({
      syncTransactions: async () => ({
        added: [
          { transaction_id: 'txn-checking', account_id: 'acc-checking', amount: 10, date: '2026-07-10', name: 'CHECK' },
          { transaction_id: 'txn-credit', account_id: 'acc-credit', amount: 20, date: '2026-07-10', name: 'CREDIT' },
        ],
        modified: [],
        removed: [],
        accounts: [
          { account_id: 'acc-checking', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 1000, iso_currency_code: 'USD' } },
          { account_id: 'acc-credit', name: 'Credit Card', type: 'credit', subtype: 'credit card', balances: { current: -200, iso_currency_code: 'USD' } },
        ],
        next_cursor: 'cursor-1',
        has_more: false,
      }),
      getLiabilities: async () => ({ credit: [{ account_id: 'acc-credit' }] }),
    })
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-fallback',
      sourceResourceIncarnationIds: {
        'acc-checking': 'src-checking',
        'acc-credit': 'src-credit',
      },
      accountIds: ['acc-checking', 'acc-credit'],
    }, transport)
    const txnObs = result.observations.filter(o => o.providerVersion.startsWith('plaid:txn:'))
    expect(txnObs).toHaveLength(2)
    const checkingTxn = txnObs.find(o => o.payload.accountId === 'acc-checking')!
    const creditTxn = txnObs.find(o => o.payload.accountId === 'acc-credit')!
    expect(checkingTxn.sourceResourceIncarnationId).toBe('src-checking')
    expect(creditTxn.sourceResourceIncarnationId).toBe('src-credit')
    const balanceObs = result.observations.filter(o => o.providerVersion.startsWith('plaid:balance:'))
    expect(balanceObs).toHaveLength(2)
    expect(balanceObs.find(o => o.payload.accountId === 'acc-checking')!.sourceResourceIncarnationId).toBe('src-checking')
    expect(balanceObs.find(o => o.payload.accountId === 'acc-credit')!.sourceResourceIncarnationId).toBe('src-credit')
    const liabilityObs = result.observations.filter(o => o.providerVersion.startsWith('plaid:liability:'))
    expect(liabilityObs).toHaveLength(1)
    expect(liabilityObs[0].payload.accountId).toBe('acc-credit')
    expect(liabilityObs[0].sourceResourceIncarnationId).toBe('src-credit')
  })

  it('counts ingestion drops instead of filtering rows silently', async () => {
    const transport = mockTransport({
      syncTransactions: async (_accessToken, cursor) => cursor
        ? { added: [], modified: [], removed: [], accounts: [], next_cursor: cursor, has_more: false }
        : {
          added: [
            { transaction_id: 'txn-admitted', account_id: 'acc-1', amount: 5, date: '2026-07-10', name: 'ADMITTED' },
            { transaction_id: 'txn-unadmitted', account_id: 'acc-other', amount: 5, date: '2026-07-10', name: 'UNADMITTED' },
          ],
          modified: [
            { transaction_id: 'txn-modified-unadmitted', account_id: 'acc-other', amount: 7, date: '2026-07-10', name: 'UNADMITTED' },
          ],
          removed: [],
          accounts: [
            { account_id: 'acc-1', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 1000, iso_currency_code: 'USD' } },
          ],
          next_cursor: 'cursor-1',
          has_more: false,
        },
    })
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
    }, transport)
    expect(result.counts.added).toBe(1)
    expect(result.counts.modified).toBe(0)
    expect(result.counts.skipped.unadmittedAccount).toBe(2)
    expect(transactionObservations(result)).toHaveLength(1)
    expect(transactionObservations(result)[0].payload.transactionId).toBe('txn-admitted')
  })
})
