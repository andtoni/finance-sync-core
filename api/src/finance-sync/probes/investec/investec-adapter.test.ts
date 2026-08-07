
import { describe, expect, it } from 'vitest'
import {
  investecPrimeSaverSupersessionKey,
  runInvestecAdapter,
  investecResourceKind,
  type InvestecTransport,
} from './investec-adapter'

const BASE_CONFIG = {
  deploymentId: 'g5-test', localOwnerId: 'alexis',
  sourceResourceIncarnationId: 'investec:connection-1',
  fromDate: '2026-07-01', toDate: '2026-07-31',
  eligibleProductNames: ['Private Bank Account'],
}

function makeTransport(txns: Record<string, unknown>[], nextCursor?: string): InvestecTransport {
  return {
    listAccounts: async () => [{ accountId: 'acc-cash-123456789', productName: 'Private Bank Account', accountName: 'A Example' }],
    getBalance: async () => ({ currency: 'ZAR', currentBalance: 25000 }),
    getTransactionPage: async () => ({ transactions: txns as any[], nextCursor }),
  }
}

function transactionObservations(result: Awaited<ReturnType<typeof runInvestecAdapter>>) {
  return result.observations.filter(observation => observation.providerVersion.startsWith('investec:txn:'))
}

describe('Investec adapter G5', () => {
  it('maps to cash_ledger', () => expect(investecResourceKind()).toBe('cash_ledger'))

  it('discovers eligible account with currency', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport([], undefined))
    expect(r.accounts.length).toBe(1)
    expect(r.accounts[0].currency).toBe('ZAR')
  })

  it('deduplicates provider rows and lets explicit admission own product eligibility', async () => {
    const t = makeTransport([
      { uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'A' },
    ])
    t.listAccounts = async () => [
      { accountId: 'acc-cash-123456789', productName: 'PrimeSaver' },
      { accountId: 'acc-cash-123456789', productName: 'PrimeSaver' },
    ]
    let balances = 0
    let transactionPages = 0
    t.getBalance = async () => { balances++; return { currency: 'ZAR', currentBalance: 25000 } }
    t.getTransactionPage = async () => { transactionPages++; return { transactions: [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'A' }] } }

    const r = await runInvestecAdapter({ ...BASE_CONFIG, accountIds: ['acc-cash-123456789'] }, t)
    expect(r.accounts).toHaveLength(1)
    expect(transactionObservations(r)).toHaveLength(1)
    expect(balances).toBe(1)
    expect(transactionPages).toBe(1)
  })

  it('skips ineligible products', async () => {
    const t = makeTransport([], undefined)
    t.listAccounts = async () => [{ accountId: 'x', productName: 'Home Loan' }]
    const r = await runInvestecAdapter({ ...BASE_CONFIG, eligibleProductNames: ['Private Bank Account'] }, t)
    expect(r.accounts.length).toBe(0)
  })

  it('quarantines missing currency', async () => {
    const t = makeTransport([], undefined)
    t.getBalance = async () => ({})
    const r = await runInvestecAdapter(BASE_CONFIG, t)
    expect(r.quarantine.some(q => q.reason === 'MISSING_ACCOUNT_CURRENCY')).toBe(true)
  })

  it('projects booked transactions and quarantines polling pendings', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport([
      { uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'A' },
      { uuid: 't2', status: 'PENDING', postedOrder: 0, type: 'DEBIT', amount: 25, transactionDate: '2026-07-11', description: 'B' },
    ], undefined))
    expect(transactionObservations(r)).toHaveLength(1)
    expect(r.quarantine.some(item => item.reason === 'PENDING_NOT_PROJECTED')).toBe(true)
  })

  it('quarantines postedOrder=0 as provisional', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 0, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'P' }], undefined))
    expect(transactionObservations(r)).toHaveLength(0)
    expect(r.quarantine[0].reason).toBe('POSTED_ORDER_ZERO_PROVISIONAL')
  })

  it('quarantines a pending polling row without projecting it', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'PENDING', postedOrder: 0, type: 'DEBIT', amount: 100, transactionDate: '2026-07-10' }], undefined))
    expect(transactionObservations(r)).toHaveLength(0)
    expect(r.quarantine).toEqual([{ reason: 'PENDING_NOT_PROJECTED', accountRef: '…6789', transactionRef: 't1' }])
  })

  it('signs DEBIT negative CREDIT positive', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport([
      { uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10' },
      { uuid: 't2', status: 'POSTED', postedOrder: 2, type: 'CREDIT', amount: 50, postingDate: '2026-07-11' },
    ], undefined))
    const amounts = transactionObservations(r).map(o => Number((o.payload as any).amount))
    expect(amounts[0]).toBeLessThan(0)
    expect(amounts[1]).toBeGreaterThan(0)
  })

  it('produces stable SHA-256 hash', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'S' }], undefined))
    expect(r.observations[0].semanticHash).toMatch(/^[a-f0-9]{64}$/)
  })

  it('deterministic hash', async () => {
    const t = [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'D' }]
    const r1 = await runInvestecAdapter(BASE_CONFIG, makeTransport(t, undefined))
    const r2 = await runInvestecAdapter(BASE_CONFIG, makeTransport(t, undefined))
    expect(r1.observations[0].semanticHash).toBe(r2.observations[0].semanticHash)
  })

  it('advances the semantic hash when provider-owned settlement evidence changes', async () => {
    const first = await runInvestecAdapter(BASE_CONFIG, makeTransport([
      { uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: '100', postingDate: '2026-07-10', feeAmount: '0.00' },
    ], undefined))
    const second = await runInvestecAdapter(BASE_CONFIG, makeTransport([
      { uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: '100', postingDate: '2026-07-10', feeAmount: '1.00' },
    ], undefined))
    expect(transactionObservations(first)[0].semanticHash).not.toBe(transactionObservations(second)[0].semanticHash)
  })

  it('redacts accountRef in payload', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'R' }], undefined))
    for (const obs of transactionObservations(r)) {
      expect((obs.payload as any).accountRef).toMatch(/^\u2026\d{4}$/)
    }
  })

  it('preserves raw accountId in payload', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'P' }], undefined))
    const [transaction] = transactionObservations(r)
    expect((transaction.payload as any).accountId).toBeDefined()
    expect((transaction.payload as any).accountRef).toMatch(/^\u2026/)
  })

  it('handles single page completion', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'P1' }], undefined))
    expect(r.pagesPolled).toBe(1)
    expect(r.paginationComplete).toBe(true)
  })

  it('quarantines unknown status', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'UNKNOWN', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10' }], undefined))
    expect(r.quarantine[0].reason).toBe('UNKNOWN_OR_UNBOOKED_STATUS')
  })

  it('quarantines missing identity', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10' }], undefined))
    expect(r.quarantine[0].reason).toBe('MISSING_IDENTITY')
  })

  it('quarantines missing date', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100 }], undefined))
    expect(r.quarantine[0].reason).toBe('MISSING_DATE')
  })

  it('reports transactionRef in quarantine when uuid present', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 'txn-zero-uuid', status: 'POSTED', postedOrder: 0, type: 'DEBIT', amount: 300, postingDate: '2026-07-12' }], undefined))
    expect(r.quarantine[0].reason).toBe('POSTED_ORDER_ZERO_PROVISIONAL')
    expect((r.quarantine[0] as any).transactionRef).toBe('\u2026uuid')
  })

  it('preserves provider type, all provider dates, and pocket grouping evidence', async () => {
    const transport = makeTransport([{
      uuid: 'pocket-1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', transactionType: 'MCPBAPocketTransfers', amount: '10.00',
      transactionDate: '2026-08-05', postingDate: '2026-08-06', actionDate: '2026-08-05', valueDate: '2026-08-06',
      description: 'SYNTHETIC USD POCKET', reference: 'pocket-ref', pocketGroupId: 'pocket-batch-1',
      feeAmount: '0.10', taxAmount: '0.01', exchangeRate: '18.00',
    } as any], undefined)
    const [observation] = transactionObservations(await runInvestecAdapter(BASE_CONFIG, transport))
    expect(observation.payload).toMatchObject({
      type: 'DEBIT', transactionType: 'MCPBAPOCKETTRANSFERS', subtype: 'POCKET_TRANSFER',
      date: '2026-08-05', transactionDate: '2026-08-05', postingDate: '2026-08-06',
      actionDate: '2026-08-05', valueDate: '2026-08-06', settlementDate: '2026-08-06',
      sourceGroupId: 'investec:pocket:investec:connection-1:pocket-batch-1',
      fee: '0.10', tax: '0.01', fxRate: '18.00', needsReview: true,
      metadata: { providerOwned: { investec: { currencyEvidence: {
        accountCurrency: 'ZAR', originalAmount: null, originalCurrency: null,
        fxRate: '18.00', status: 'account_only',
      } } } },
    })
    expect(observation.observedAt).not.toBe('2026-08-05T00:00:00.000Z')
    expect(observation.rawPayload).toMatchObject({ uuid: 'pocket-1', feeAmount: '0.10', exchangeRate: '18.00' })
  })

  it('accepts PrimeSaver booked rows without UUIDs using stable synthetic identities', async () => {
    const txns = [
      {
        uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
        postingDate: '2026-07-12', actionDate: '2026-07-11', transactionDate: '2026-07-11',
        description: 'PrimeSaver interest', reference: 'MBP000001', runningBalance: '10300',
      },
      {
        uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
        postingDate: '2026-07-12', actionDate: '2026-07-11', transactionDate: '2026-07-11',
        description: 'PrimeSaver interest', reference: 'MBP000002', runningBalance: '10600',
      },
    ]
    const transport = makeTransport(txns, undefined)
    transport.listAccounts = async () => [{ accountId: 'acc-cash-123456789', productName: 'PrimeSaver', accountName: 'Prime Saver' }]
    const config = { ...BASE_CONFIG, accountIds: ['acc-cash-123456789'] }
    const first = await runInvestecAdapter(config, transport)
    const second = await runInvestecAdapter(config, transport)
    const firstTransactions = transactionObservations(first)
    const secondTransactions = transactionObservations(second)

    expect(firstTransactions).toHaveLength(2)
    expect(first.quarantine).toHaveLength(0)
    expect(firstTransactions.map(item => (item.payload as any).date)).toEqual(['2026-07-11', '2026-07-11'])
    expect(firstTransactions.map(item => item.providerVersion)).toEqual(secondTransactions.map(item => item.providerVersion))
    expect(firstTransactions.every(item => String(item.payload.transactionId).match(/^synthetic-v2:[a-f0-9]{64}$/))).toBe(true)
  })

  it('matches legacy fallback observations when only the provider date changes', () => {
    const first = investecPrimeSaverSupersessionKey({
      accountId: 'acc-cash-123456789', amount: '107.49', transactionType: 'CREDIT',
      description: 'Interest Value Date 01Jul26', cardNumber: null, date: '2026-06-30',
    })
    const changedDate = investecPrimeSaverSupersessionKey({
      accountId: 'acc-cash-123456789', amount: '107.49', transactionType: 'CREDIT',
      description: 'Interest Value Date 01Jul26', cardNumber: null, date: '2026-08-04',
    })
    const changedNarrative = investecPrimeSaverSupersessionKey({
      accountId: 'acc-cash-123456789', amount: '107.49', transactionType: 'CREDIT',
      description: 'Interest Value Date 02Jul26', cardNumber: null, date: '2026-06-30',
    })
    expect(first).toBeTruthy()
    expect(changedDate).toBe(first)
    expect(changedNarrative).not.toBe(first)
  })

  it('keeps a reference-less PrimeSaver identity stable when only booking dates change', async () => {
    const transport = makeTransport([{
      uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
      postingDate: '2026-07-12', valueDate: '2026-07-11', transactionDate: '2026-07-11', description: 'Interest',
    }], undefined)
    transport.listAccounts = async () => [{ accountId: 'acc-cash-123456789', productName: 'PrimeSaver', accountName: 'Prime Saver' }]
    const config = { ...BASE_CONFIG, accountIds: ['acc-cash-123456789'] }
    const first = await runInvestecAdapter(config, transport)
    transport.getTransactionPage = async () => ({ transactions: [{
      uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
      postingDate: '2026-07-15', valueDate: '2026-07-14', transactionDate: '2026-07-11', description: 'Interest',
    }] })
    const second = await runInvestecAdapter(config, transport)
    expect(transactionObservations(second)[0].providerVersion).toBe(transactionObservations(first)[0].providerVersion)
  })

  it('keeps a PrimeSaver identity stable when mutable provider dates and balance change', async () => {
    const transport = makeTransport([{
      uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
      postingDate: '2026-07-12', actionDate: '2026-07-11', transactionDate: '2026-07-11',
      valueDate: '2026-07-11', description: 'Internal Transaction MBP000123', runningBalance: '10300',
    }], undefined)
    transport.listAccounts = async () => [{ accountId: 'acc-cash-123456789', productName: 'PrimeSaver', accountName: 'Prime Saver' }]
    const config = { ...BASE_CONFIG, accountIds: ['acc-cash-123456789'] }
    const first = await runInvestecAdapter(config, transport)
    transport.getTransactionPage = async () => ({ transactions: [{
      uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
      postingDate: '2026-07-15', actionDate: '2026-07-14', transactionDate: '2026-07-14',
      valueDate: '2026-07-14', description: 'Internal Transaction MBP000123', runningBalance: '10600',
    }] })
    const second = await runInvestecAdapter(config, transport)
    const firstTransaction = transactionObservations(first)[0]
    const secondTransaction = transactionObservations(second)[0]
    expect(secondTransaction.providerVersion).toBe(firstTransaction.providerVersion)
    expect(secondTransaction.payload.primeSaverIdentityKey).toBe(firstTransaction.payload.primeSaverIdentityKey)
  })

  it('quarantines reference-less PrimeSaver rows without a provider transaction date', async () => {
    const transport = makeTransport([{
      uuid: '', status: 'POSTED', postedOrder: 0, type: 'CREDIT', amount: '300',
      postingDate: '2026-07-12', description: 'Interest',
    }], undefined)
    transport.listAccounts = async () => [{ accountId: 'acc-cash-123456789', productName: 'PrimeSaver', accountName: 'Prime Saver' }]
    const result = await runInvestecAdapter({ ...BASE_CONFIG, accountIds: ['acc-cash-123456789'] }, transport)
    expect(transactionObservations(result)).toHaveLength(0)
    expect(result.quarantine.some(item => item.reason === 'MISSING_IDENTITY')).toBe(true)
  })

  it('keeps UUID-less postedOrder=0 rows quarantined for other Investec products', async () => {
    const r = await runInvestecAdapter({ ...BASE_CONFIG, accountIds: ['acc-cash-123456789'] }, makeTransport(
      [{ uuid: '', status: 'POSTED', postedOrder: 0, type: 'DEBIT', amount: 300, postingDate: '2026-07-12' }], undefined))
    expect(transactionObservations(r)).toHaveLength(0)
    expect(r.quarantine[0].reason).toBe('MISSING_IDENTITY')
  })

  it('retains validated provider decimal strings without Number/toFixed truncation', async () => {
    const transport = makeTransport([
      { uuid: 'precise', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: '123456789.123456789123456789', postingDate: '2026-07-10' },
    ], undefined)
    transport.getBalance = async () => ({ currency: 'ZAR', currentBalance: '987654321.987654321987654321' })
    const r = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(r.observations.find(item => item.domain === 'balance')?.payload.currentBalance).toBe('987654321.987654321987654321')
    expect(transactionObservations(r)[0].payload.amount).toBe('-123456789.123456789123456789')
  })

  it('handles empty response', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport([], undefined))
    expect(transactionObservations(r)).toHaveLength(0)
    expect(r.observations).toHaveLength(1) // complete current-balance snapshot
    expect(r.paginationComplete).toBe(true)
  })
})

it('timestamps changed same-day balances at successful fetch time so the newer state can replace', async () => {
  let currentBalance = '10'
  const transport = {
    listAccounts: async () => [{ accountId: 'account-1', productName: 'Private Bank' }],
    getBalance: async () => ({ currency: 'ZAR', currentBalance }),
    getTransactionPage: async () => ({ transactions: [] }),
  }
  const config = {
    deploymentId: 'dep', localOwnerId: 'issuer#owner',
    sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
    fromDate: '2026-01-01', toDate: '2026-07-18', eligibleProductNames: ['Private Bank'],
  }
  const first = await runInvestecAdapter(config, transport)
  await new Promise(resolve => setTimeout(resolve, 2))
  currentBalance = '11'
  const second = await runInvestecAdapter(config, transport)
  const firstBalance = first.observations.find(item => item.domain === 'balance')!
  const secondBalance = second.observations.find(item => item.domain === 'balance')!
  expect(firstBalance.observedAt).not.toBe('2026-07-18T00:00:00.000Z')
  expect(Date.parse(secondBalance.observedAt)).toBeGreaterThan(Date.parse(firstBalance.observedAt))
  expect(secondBalance.semanticHash).not.toBe(firstBalance.semanticHash)
})
