
import { describe, expect, it, vi } from 'vitest'
import { plaidResourceKind, runPlaidSync } from './plaid-adapter'
import type { PlaidTransport } from './plaid-adapter'
import { providerObservationToBridge } from '../../projection/bridge-client'

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
      accessToken: 'access-sandbox-abc', deploymentId: 'g4-test', localOwnerId: 'urn:test#alexis',
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

  it('skips only the balance facts of an account without a current balance and keeps its transactions', async () => {
    const balances = (current: number | null) => ({ current, iso_currency_code: 'USD' })
    const transport = mockTransport({
      syncTransactions: async () => ({
        added: [
          { transaction_id: 'txn-checking', account_id: 'acc-checking', amount: 10, date: '2026-07-10', name: 'CHECK' },
          { transaction_id: 'txn-credit', account_id: 'acc-credit', amount: 20, date: '2026-07-10', name: 'CREDIT' },
          { transaction_id: 'txn-savings', account_id: 'acc-savings', amount: 30, date: '2026-07-10', name: 'SAVE' },
        ],
        modified: [], removed: [],
        accounts: [
          { account_id: 'acc-checking', name: 'Checking', type: 'depository', subtype: 'checking', balances: balances(null) },
          { account_id: 'acc-credit', name: 'Credit Card', type: 'credit', subtype: 'credit card', balances: balances(Number.NaN) },
          { account_id: 'acc-savings', name: 'Savings', type: 'depository', subtype: 'savings', balances: balances(1000) },
        ],
        next_cursor: 'cursor-1', has_more: false,
      }),
      getLiabilities: async () => ({ credit: [{ account_id: 'acc-credit' }] }),
    })
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc', deploymentId: 'g4-test', localOwnerId: 'urn:test#alexis',
      sourceResourceIncarnationId: 'plaid:item-1', accountIds: ['acc-checking', 'acc-credit', 'acc-savings'],
    }, transport)

    expect(result.nextCursor).toBe('cursor-1')
    expect(transactionObservations(result).map(observation => observation.payload.accountId).sort())
      .toEqual(['acc-checking', 'acc-credit', 'acc-savings'])
    // Only the account with a real balance gets a balance fact; nothing is invented for the others.
    const stateFacts = result.observations.filter(observation => !observation.providerVersion.startsWith('plaid:txn:'))
    expect(stateFacts.map(observation => observation.providerVersion)).toEqual(['plaid:balance:acc-savings'])
    expect(stateFacts[0].payload.currentBalance).toBe('1000')
    expect(result.observations.filter(observation => 'currentBalance' in observation.payload)
      .map(observation => observation.payload.accountId)).toEqual(['acc-savings'])
    expect(result.accountsWithoutBalance).toEqual(['acc-checking', 'acc-credit'])
    expect(result.counts.skipped.missingBalance).toBe(2)
  })

  it('emits nothing for an admitted account Plaid did not return and holds the cursor', async () => {
    const transport = mockTransport({
      syncTransactions: async () => ({
        added: [
          { transaction_id: 'txn-present', account_id: 'acc-present', amount: 10, date: '2026-07-10', name: 'PRESENT', iso_currency_code: 'USD' },
          { transaction_id: 'txn-absent', account_id: 'acc-absent', amount: 20, date: '2026-07-10', name: 'ABSENT', iso_currency_code: 'USD' },
        ],
        modified: [{ transaction_id: 'txn-absent-modified', account_id: 'acc-absent', amount: 30, date: '2026-07-10', name: 'ABSENT', iso_currency_code: 'USD' }],
        removed: [],
        accounts: [{ account_id: 'acc-present', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 1000, iso_currency_code: 'USD' } }],
        next_cursor: 'cursor-1', has_more: false,
      }),
    })
    const config = {
      accessToken: 'access-sandbox-abc', deploymentId: 'g4-test', localOwnerId: 'urn:test#alexis',
      sourceResourceIncarnationId: 'plaid:item-1', accountIds: ['acc-present', 'acc-absent'],
    }
    const result = await runPlaidSync({ ...config, initialCursor: 'cursor-0' }, transport)

    expect(result.accountsNotReturned).toEqual(['acc-absent'])
    // The account's type is unknown, so none of its rows are classified or emitted, and no balance is invented.
    expect(result.observations.map(observation => observation.providerVersion)).toEqual(['plaid:txn:txn-present', 'plaid:balance:acc-present'])
    expect(result.observations.some(observation => observation.payload.accountId === 'acc-absent')).toBe(false)
    expect(result.counts).toMatchObject({ added: 1, modified: 0, skipped: { accountNotReturned: 1, missingBalance: 0 } })
    // The cursor does not move past rows that were not ingested.
    expect(result.nextCursor).toBe('cursor-0')
    await expect(runPlaidSync(config, transport)).resolves.toMatchObject({ nextCursor: null, accountsNotReturned: ['acc-absent'] })
  })

  describe('an account the institution re-issued under a new account id', () => {
    const account = (accountId: string, mask: string, name: string) =>
      ({ account_id: accountId, name, mask, type: 'depository', subtype: 'checking', balances: { current: 365, iso_currency_code: 'USD' } })
    const transportReturning = (accounts: ReturnType<typeof account>[], accountIdOfTransaction: string): PlaidTransport => ({
      getAccounts: async () => accounts,
      syncTransactions: async () => ({
        added: [{ transaction_id: 'txn-new', account_id: accountIdOfTransaction, amount: 20, date: '2026-10-01', name: 'COFFEE' }],
        modified: [], removed: [], accounts: [], next_cursor: 'cursor-1', has_more: false,
      }),
    })
    const config = {
      accessToken: 'access', deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: 'item',
      accountIds: ['acc-old'], sourceResourceIncarnationIds: { 'acc-old': 'source-old' },
      reissueCandidates: { 'acc-old': { mask: '0042', name: 'Everyday Checking', currency: 'USD', sourceResourceIncarnationId: 'source-old' } },
    }

    it('ingests it as the admitted account and advances the cursor', async () => {
      const result = await runPlaidSync(config, transportReturning([account('acc-new', '0042', 'Everyday Checking')], 'acc-new'))
      expect(result.accountsNotReturned).toEqual([])
      expect(result.accountsReissued).toEqual({ 'acc-old': 'acc-new' })
      expect(result.nextCursor).toBe('cursor-1')
      expect(result.counts.skipped.unadmittedAccount).toBe(0)
      const [transaction] = transactionObservations(result)
      expect(transaction).toMatchObject({ sourceResourceIncarnationId: 'source-old', payload: { transactionId: 'txn-new', accountId: 'acc-old' } })
      // The provider's own account id stays in the raw evidence.
      expect(transaction.rawPayload).toMatchObject({ account_id: 'acc-new' })
      const balance = result.observations.find(observation => observation.providerVersion === 'plaid:balance:acc-old')
      expect(balance).toMatchObject({ sourceResourceIncarnationId: 'source-old', payload: { accountId: 'acc-old', currentBalance: '365' } })
    })

    it('ingests an account of another Item of the same owner under that account\'s own source', async () => {
      const result = await runPlaidSync({
        ...config, accountIds: ['acc-home'], sourceResourceIncarnationIds: { 'acc-home': 'source-home' },
      }, transportReturning([account('acc-home', '0020', 'Savings'), account('acc-new', '0042', 'Everyday Checking')], 'acc-new'))
      expect(result.accountsNotReturned).toEqual([])
      expect(result.accountsReissued).toEqual({ 'acc-old': 'acc-new' })
      expect(transactionObservations(result)[0]).toMatchObject({ sourceResourceIncarnationId: 'source-old', payload: { accountId: 'acc-old' } })
      expect(result.observations.some(observation => observation.providerVersion === 'plaid:balance:acc-old')).toBe(true)
    })

    it('leaves the account unadmitted when the match is ambiguous or differs', async () => {
      for (const accounts of [
        [account('acc-new', '0042', 'Everyday Checking'), account('acc-other', '0042', 'Everyday Checking')],
        [account('acc-new', '0042', 'Another Account')],
        [account('acc-new', '9999', 'Everyday Checking')],
      ]) {
        const result = await runPlaidSync(config, transportReturning(accounts, 'acc-new'))
        expect(result.accountsReissued).toEqual({})
        expect(result.accountsNotReturned).toEqual(['acc-old'])
        expect(transactionObservations(result)).toHaveLength(0)
        expect(result.counts.skipped.unadmittedAccount).toBe(1)
      }
    })

    it('never re-homes an account that is still returned under its own id', async () => {
      const result = await runPlaidSync(config, transportReturning(
        [account('acc-old', '0042', 'Everyday Checking'), account('acc-new', '0042', 'Everyday Checking')], 'acc-new'))
      expect(result.accountsReissued).toEqual({})
      expect(transactionObservations(result)).toHaveLength(0)
    })
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

describe('money into a Plaid credit card', () => {
  const card = { account_id: 'acc-card', name: 'Cashback Card', type: 'credit', subtype: 'credit card', balances: { current: 300, iso_currency_code: 'USD' } }
  const checking = { account_id: 'acc-checking', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 1000, iso_currency_code: 'USD' } }
  const refund = {
    transaction_id: 'txn-refund', account_id: 'acc-card', amount: -115, date: '2026-10-21', name: 'SAASCO* SUBSCRIPTION',
    transaction_code: 'adjustment', personal_finance_category: { primary: 'INCOME', detailed: 'INCOME_OTHER_INCOME' },
  }
  const payment = {
    transaction_id: 'txn-payment', account_id: 'acc-card', amount: -192.25, date: '2026-10-14', name: 'BANK A MOBILE PYMT',
    transaction_code: 'payment', personal_finance_category: { primary: 'LOAN_PAYMENTS', detailed: 'LOAN_PAYMENTS_CREDIT_CARD_PAYMENT' },
  }
  const uncodedPayment = {
    transaction_id: 'txn-uncoded-payment', account_id: 'acc-card', amount: -50, date: '2026-10-15', name: 'AUTOPAY',
    personal_finance_category: { primary: 'LOAN_PAYMENTS', detailed: 'LOAN_PAYMENTS_CREDIT_CARD_PAYMENT' },
  }
  const purchase = {
    transaction_id: 'txn-purchase', account_id: 'acc-card', amount: 115, date: '2026-10-01', name: 'SAASCO* SUBSCRIPTION',
    transaction_code: 'purchase',
  }
  const deposit = {
    transaction_id: 'txn-deposit', account_id: 'acc-checking', amount: -50, date: '2026-10-15', name: 'BANK B NA',
    transaction_code: 'adjustment', personal_finance_category: { primary: 'TRANSFER_IN' },
  }

  async function projected(page: { added?: readonly unknown[]; modified?: readonly unknown[] }) {
    const result = await runPlaidSync({
      accessToken: 'access-sandbox-abc', deploymentId: 'g4-test', localOwnerId: 'urn:test#alexis',
      sourceResourceIncarnationId: 'plaid:item-1', accountIds: ['acc-card', 'acc-checking'],
    }, mockTransport({
      syncTransactions: async () => ({
        added: (page.added ?? []) as never, modified: (page.modified ?? []) as never, removed: [],
        accounts: [card, checking], next_cursor: 'cursor-1', has_more: false,
      }),
    }))
    return new Map(transactionObservations(result).map(observation => [
      String(observation.payload.transactionId),
      providerObservationToBridge('plaid', observation, 1, 'wf-account')?.activity,
    ]))
  }

  it('projects a refund as a credit to the card and leaves payments, purchases and bank deposits as they were', async () => {
    const activities = await projected({ added: [refund, payment, uncodedPayment, purchase, deposit] })
    expect(activities.get('txn-refund')).toMatchObject({ activityType: 'CREDIT', subtype: 'REFUND', amount: '115' })
    expect(activities.get('txn-payment')).toMatchObject({ activityType: 'DEPOSIT', amount: '192.25' })
    expect(activities.get('txn-payment')?.subtype).toBeUndefined()
    expect(activities.get('txn-uncoded-payment')).toMatchObject({ activityType: 'DEPOSIT' })
    expect(activities.get('txn-purchase')).toMatchObject({ activityType: 'WITHDRAWAL' })
    expect(activities.get('txn-deposit')).toMatchObject({ activityType: 'DEPOSIT' })
  })

  it('projects a modified refund as a credit to the card', async () => {
    const activities = await projected({ modified: [refund, payment] })
    expect(activities.get('txn-refund')).toMatchObject({ activityType: 'CREDIT', subtype: 'REFUND' })
    expect(activities.get('txn-payment')).toMatchObject({ activityType: 'DEPOSIT' })
  })
})
