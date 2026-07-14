
import { describe, expect, it, vi } from 'vitest'
import { plaidResourceKind, runPlaidSync } from './plaid-adapter'
import type { PlaidTransport } from './plaid-adapter'

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
    expect(result.observations.length).toBe(2)
    expect(result.observations[0].payload).toMatchObject({
      transactionId: 'txn-1',
      accountId: 'acc-1',
    })
    expect(result.observations[1].payload.transactionId).toBe('txn-2')
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
      return { added: [], modified: [], removed: [], accounts: [{ account_id: 'acc-1', name: 'Mock', type: 'depository', subtype: 'checking', balances: { current: null, iso_currency_code: 'USD' } }], next_cursor: 'cursor-restart', has_more: false }
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

  it('quarantines observations with missing currency', async () => {
    const transport = mockTransport({
      syncTransactions: async () => ({
        added: [{ transaction_id: 'txn-no-ccy', account_id: 'acc-1', amount: 100, date: '2026-07-10', name: 'NOCCY' }],
        modified: [], removed: [],
        accounts: [{ account_id: 'acc-1', name: 'Unknown', type: 'depository', subtype: 'checking', balances: { current: null, iso_currency_code: null } }],
        next_cursor: 'cursor-x', has_more: false,
      }),
    })
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc',
      deploymentId: 'g4-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
    }, transport)
    expect(result.observations.length).toBe(0)
  })
})
