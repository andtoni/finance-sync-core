
import { describe, expect, it } from 'vitest'
import { runInvestecAdapter, investecResourceKind, type InvestecTransport } from './investec-adapter'

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

  it('produces observations for posted transactions only', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'A' }], undefined))
    expect(transactionObservations(r)).toHaveLength(1)
  })

  it('quarantines postedOrder=0 as provisional', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'POSTED', postedOrder: 0, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'P' }], undefined))
    expect(transactionObservations(r)).toHaveLength(0)
    expect(r.quarantine[0].reason).toBe('POSTED_ORDER_ZERO_PROVISIONAL')
  })

  it('skips pending without quarantine', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ uuid: 't1', status: 'PENDING', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10' }], undefined))
    expect(transactionObservations(r)).toHaveLength(0)
    expect(r.quarantine.length).toBe(0)
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

  it('no transactionRef when uuid absent', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport(
      [{ status: 'POSTED', postedOrder: 0, type: 'DEBIT', amount: 300, postingDate: '2026-07-12' }], undefined))
    expect((r.quarantine[0] as any).transactionRef).toBeUndefined()
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
