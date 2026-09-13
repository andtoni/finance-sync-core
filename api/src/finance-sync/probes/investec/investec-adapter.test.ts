
import { describe, expect, it, vi } from 'vitest'
import { investecCardObservation } from '../../investec-card-observation'
import {
  investecPrimeSaverSupersessionKey,
  runInvestecAdapter,
  investecResourceKind,
  correlateInvestecTransferObservations,
  finalizeInvestecObservations,
  type InvestecTransport,
} from './investec-adapter'

const BASE_CONFIG = {
  deploymentId: 'g5-test', localOwnerId: 'alexis',
  sourceResourceIncarnationId: 'investec:connection-1',
  fromDate: '2026-07-01', toDate: '2026-07-31',
  eligibleProductNames: ['Private Bank Account'],
}

function makeTransport(txns: Record<string, unknown>[], nextCursor?: string, accounts: readonly { accountId: string; productName?: string; accountName?: string; accountNumber?: string }[] = [{ accountId: 'acc-cash-123456789', productName: 'Private Bank Account', accountName: 'A Example' }]): InvestecTransport {
  return {
    listAccounts: async () => accounts,
    getBalance: async () => ({ currency: 'ZAR', currentBalance: 25000 }),
    getTransactionPage: async () => ({ transactions: txns as any[], nextCursor }),
  }
}

function transactionObservations(result: Awaited<ReturnType<typeof runInvestecAdapter>>) {
  return result.observations.filter(observation => observation.providerVersion.startsWith('investec:txn:'))
}

function executionObservations(result: Awaited<ReturnType<typeof runInvestecAdapter>>) {
  return result.observations.filter(observation => observation.domain === 'provider_audit' || observation.payload.status === 'pending')
}

describe('Investec adapter G5', () => {
  it('execution recovery and webhook normalization share one card identity and hash', async () => {
    const payload = { accountNumber: '10001111', centsAmount: 1299, currencyCode: 'USD', dateTime: '2026-07-10T12:00:00Z', reference: 'same-card', merchant: { name: 'Shop' }, card: { display: '4111111111111111' } }
    const webhook = investecCardObservation({ deploymentId: BASE_CONFIG.deploymentId, localOwnerId: BASE_CONFIG.localOwnerId, sourceResourceIncarnationId: BASE_CONFIG.sourceResourceIncarnationId, providerAccountId: 'acc-1', accountCurrency: 'ZAR', payload })
    const transport = makeTransport([], undefined, [{ accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' }])
    transport.listCards = async () => [{ cardKey: 'card-1', accountNumber: '10001111' }]
    transport.listCardExecutions = async () => [{ executionId: 'exec-1', sandbox: false, type: 'after_transaction', authorizationApproved: true, completedAt: '2026-07-10T12:00:01Z', logs: [{ content: JSON.stringify({ schemaVersion: 1, eventType: 'after_transaction', providerEventTime: payload.dateTime, transaction: payload }) }] }]
    const recovered = (await runInvestecAdapter(BASE_CONFIG, transport)).observations.find(item => item.payload.status === 'pending')
    expect(recovered?.providerVersion).toBe(webhook?.providerVersion)
    expect(recovered?.semanticHash).toBe(webhook?.semanticHash)
    expect(recovered?.hashSpecificationVersion).toBe(5)
    expect(webhook?.hashSpecificationVersion).toBe(5)
  })

  it('preserves provider merchant identity as provider-owned evidence', async () => {
    const result = await runInvestecAdapter(BASE_CONFIG, makeTransport([{
      uuid: 'pb-merchant-identity', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100,
      postingDate: '2026-07-10', transactionDate: '2026-07-10', description: 'Booked label',
      merchantId: 'merchant-42', transactionType: 'CARDPURCHASES',
    }]))
    expect(transactionObservations(result)[0]?.payload).toMatchObject({
      metadata: { providerOwned: { investec: { merchantIdentity: 'merchant-42' } } },
    })
    expect(transactionObservations(result)[0]?.hashSpecificationVersion).toBe(5)
    expect(result.observations.find(observation => observation.domain === 'balance')?.hashSpecificationVersion).toBe(4)
  })

  it('maps to cash_ledger', () => expect(investecResourceKind()).toBe('cash_ledger'))

  it('discovers eligible account with currency', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport([], undefined))
    expect(r.accounts.length).toBe(1)
    expect(r.accounts[0].currency).toBe('ZAR')
  })

  it('recovers one complete production card execution as authorization and always retains audit evidence', async () => {
    let cards = 0
    let executions = 0
    const transport = makeTransport([], undefined, [{ accountId: 'acc-1', accountNumber: '1000 1111', productName: 'Private Bank Account' }])
    transport.listCards = async () => { cards++; return [{ cardKey: 'card-1', accountNumber: '10001111' }] }
    transport.listCardExecutions = async () => { executions++; return [{ executionId: 'exec-12345678', sandbox: false, type: 'after_transaction', authorizationApproved: true, completedAt: '2026-07-10T12:00:01Z', logs: [{ content: JSON.stringify({ schemaVersion: 1, eventType: 'after_transaction', providerEventTime: '2026-07-10T12:00:00Z', transaction: { accountNumber: '10001111', centsAmount: 1299, currencyCode: 'USD', dateTime: '2026-07-10T12:00:00Z', reference: 'card-ref', merchant: { name: 'Shop' }, card: { display: '4111111111111111', number: 4111111111111111 } } }) }] }] }
    const result = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(cards).toBe(1); expect(executions).toBe(1)
    const recovered = executionObservations(result).find(item => item.payload.status === 'pending')
    expect(recovered?.payload).toMatchObject({ status: 'pending', authorizationDateTime: '2026-07-10T12:00:00.000Z' })
    expect(recovered?.payload).not.toHaveProperty('settlementDate')
    expect(recovered?.providerVersion).toBe('investec:card:investec-card:acc-1:reference-card-ref')
    expect(recovered?.rawPayload).toMatchObject({ schemaVersion: 1, transaction: { accountNumber: '10001111' } })
    expect(JSON.stringify(recovered?.payload)).not.toContain('exec-12345678')
    expect(JSON.stringify(recovered?.rawPayload)).not.toContain('4111111111111111')
    const audit = executionObservations(result).find(item => item.domain === 'provider_audit')
    expect(audit?.hashSpecificationVersion).toBe(4)
    expect(JSON.stringify(audit?.rawPayload)).not.toContain('4111111111111111')
  })

  it('retains sandbox and incomplete executions as audit only', async () => {
    const transport = makeTransport([], undefined, [{ accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' }])
    transport.listCards = async () => [{ cardKey: 'card-1', accountNumber: '10001111' }]
    transport.listCardExecutions = async () => [
      { executionId: 'sandbox-12345678', sandbox: true, type: 'after_transaction', completedAt: '2026-07-10T12:00:01Z' },
      { executionId: 'incomplete-12345678', sandbox: false, type: 'after_transaction', authorizationApproved: true, completedAt: '2026-07-10T12:00:01Z', logs: [] },
    ]
    const result = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(executionObservations(result).filter(item => item.providerVersion.endsWith(':authorization'))).toHaveLength(0)
    expect(executionObservations(result)).toHaveLength(2)
    expect(executionObservations(result).map(item => (item.payload as any).reason)).toEqual(['INCOMPLETE_OR_NON_TRANSACTION', 'INCOMPLETE_OR_NON_TRANSACTION'])
  })

  it('retains ambiguous card mapping as audit only', async () => {
    const transport = makeTransport([], undefined, [
      { accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' },
      { accountId: 'acc-2', accountNumber: '10001111', productName: 'Private Bank Account' },
    ])
    transport.listCards = async () => [{ cardKey: 'card-1', accountNumber: '10001111' }]
    transport.listCardExecutions = async () => [{ executionId: 'ambiguous-12345678', sandbox: false, type: 'after_transaction', authorizationApproved: true, completedAt: '2026-07-10T12:00:01Z', logs: [{ content: JSON.stringify({ schemaVersion: 1, eventType: 'after_transaction', providerEventTime: '2026-07-10T12:00:00Z', transaction: { accountNumber: '10001111', centsAmount: 1299, currencyCode: 'USD', dateTime: '2026-07-10T12:00:00Z' } }) }] }]
    const result = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(executionObservations(result)).toHaveLength(1)
    expect((executionObservations(result)[0].payload as any).reason).toBe('AMBIGUOUS_CARD_ACCOUNT')
    expect(executionObservations(result)[0].providerVersion).not.toContain(':authorization')
  })

  it('rejects conflicting Card API account identifiers and deduplicates identical provider rows', async () => {
    const execution = {
      executionId: 'mapping-12345678', sandbox: false, type: 'after_transaction', authorizationApproved: true,
      completedAt: '2026-07-10T12:00:01Z', logs: [{ content: JSON.stringify({
        accountNumber: '10001111', centsAmount: 1299, currencyCode: 'USD',
        dateTime: '2026-07-10T12:00:00Z', reference: 'mapping-ref',
      }) }],
    }
    const conflicting = makeTransport([], undefined, [
      { accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' },
      { accountId: 'acc-2', accountNumber: '20002222', productName: 'Private Bank Account' },
    ])
    conflicting.listCards = async () => [{ cardKey: 'card-1', accountId: 'acc-1', accountNumber: '20002222' }]
    conflicting.listCardExecutions = async () => [execution]
    const conflictResult = await runInvestecAdapter(BASE_CONFIG, conflicting)
    expect((executionObservations(conflictResult)[0].payload as any).reason)
      .toBe('CONFLICTING_CARD_ACCOUNT_IDENTIFIERS')
    expect(executionObservations(conflictResult).filter(item => item.payload.status === 'pending')).toHaveLength(0)

    const duplicate = makeTransport([], undefined, [
      { accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' },
      { accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' },
    ])
    duplicate.listCards = async () => [{ cardKey: 'card-1', accountId: 'acc-1', accountNumber: '10001111' }]
    duplicate.listCardExecutions = async () => [execution]
    const duplicateResult = await runInvestecAdapter(BASE_CONFIG, duplicate)
    expect(executionObservations(duplicateResult).filter(item => item.payload.status === 'pending')).toHaveLength(1)
  })

  it('marks recovered but unnormalizable execution evidence as audit-only', async () => {
    const transport = makeTransport([], undefined, [{ accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' }])
    transport.listCards = async () => [{ cardKey: 'card-1', accountNumber: '10001111' }]
    transport.listCardExecutions = async () => [{
      executionId: 'invalid-12345678', sandbox: false, type: 'after_transaction', authorizationApproved: true,
      completedAt: '2026-07-10T12:00:01Z', logs: [{ content: JSON.stringify({
        accountNumber: '10001111', currencyCode: 'USD', dateTime: '2026-07-10T12:00:00Z',
      }) }],
    }]
    const result = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(executionObservations(result)).toHaveLength(1)
    expect(executionObservations(result)[0].payload).toMatchObject({
      outcome: 'AUDIT_ONLY', reason: 'AUTHORIZATION_NORMALIZATION_FAILED',
    })
  })

  it('emits redacted recovery diagnostics without provider error details', async () => {
    const transport = makeTransport([], undefined, [{ accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' }])
    transport.listCards = async () => { throw new Error('provider response body must not be logged') }
    transport.listCardExecutions = async () => []
    const diagnostic = vi.spyOn(console, 'error').mockImplementation(() => undefined)
    try {
      const result = await runInvestecAdapter(BASE_CONFIG, transport)
      expect(transactionObservations(result)).toHaveLength(0)
      expect(diagnostic).toHaveBeenCalledWith('FINANCE_SYNC_CARD_EXECUTION_RECOVERY_FAILED', expect.stringContaining('"stage":"card_list"'))
      expect(diagnostic.mock.calls.flat().join(' ')).not.toContain('provider response body')
      expect(diagnostic.mock.calls.flat().join(' ')).toContain('"status":"failed"')
    } finally {
      diagnostic.mockRestore()
    }
  })

  it('keeps the PB cycle healthy when the official Card API backstop is unavailable', async () => {
    const transport = makeTransport([
      { uuid: 'pb-1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'PB' },
    ], undefined, [{ accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' }])
    transport.listCards = async () => { throw new Error('scope unavailable') }
    transport.listCardExecutions = async () => []
    const result = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(transactionObservations(result)).toHaveLength(1)
    expect(result.paginationComplete).toBe(true)
  })

  it('keeps the PB cycle healthy when one card has no stable key or execution endpoint', async () => {
    const transport = makeTransport([
      { uuid: 'pb-2', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 100, postingDate: '2026-07-10', description: 'PB' },
    ], undefined, [{ accountId: 'acc-1', accountNumber: '10001111', productName: 'Private Bank Account' }])
    transport.listCards = async () => [{ cardKey: '' }, { cardKey: 'card-1', accountNumber: '10001111' }]
    transport.listCardExecutions = async () => { throw new Error('execution endpoint unavailable') }
    const result = await runInvestecAdapter(BASE_CONFIG, transport)
    expect(transactionObservations(result)).toHaveLength(1)
    expect(result.paginationComplete).toBe(true)
    expect(executionObservations(result)).toHaveLength(0)
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

  it('pairs only a unique reciprocal own-account transfer using complete account-number evidence', async () => {
    const accounts = [
      { accountId: 'acc-a', accountNumber: '1000 1111', productName: 'Private Bank Account', accountName: 'A' },
      { accountId: 'acc-b', accountNumber: '1000 2222', productName: 'Private Bank Account', accountName: 'B' },
    ]
    const transport = makeTransport([], undefined, accounts)
    transport.getTransactionPage = async (accountId) => ({ transactions: accountId === 'acc-a' ? [{ uuid: 'out-1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: '2500.00', transactionDate: '2026-07-10', description: 'Transfer to 1000 2222' }] : [{ uuid: 'in-1', status: 'POSTED', postedOrder: 1, type: 'CREDIT', amount: '2500.00', transactionDate: '2026-07-10', description: 'Transfer from 1000 1111' }] })
    const result = await runInvestecAdapter({ ...BASE_CONFIG, accountIds: ['acc-a', 'acc-b'], providerConnectionId: 'investec:connection-1' }, transport)
    const transfers = transactionObservations(result)
    expect(transfers).toHaveLength(2)
    expect(transfers.map(item => item.payload.activityType)).toEqual(['TRANSFER_OUT', 'TRANSFER_IN'])
    expect(transfers[0].payload.sourceGroupId).toBe(transfers[1].payload.sourceGroupId)
    expect(transfers[0].semanticHash).toMatch(/^[a-f0-9]{64}$/)
    const singletonTopology = finalizeInvestecObservations(correlateInvestecTransferObservations(
      transfers,
      { 'acc-a': '10001111', 'acc-b': '10002222' },
      'investec:connection-1',
    ))
    expect(singletonTopology.map(item => item.semanticHash)).toEqual(transfers.map(item => item.semanticHash))
  })

  it('correlates frozen singleton observations without mutating the source payloads', () => {
    const make = (accountId: string, accountNumber: string, direction: string, id: string) => Object.freeze({
      deploymentId: 'g5-test', localOwnerId: 'alexis', sourceResourceIncarnationId: `investec:${accountId}`,
      providerVersion: `investec:txn:${id}`, semanticHash: 'a'.repeat(64), hashSpecificationVersion: 4,
      domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z',
      payload: Object.freeze({ accountId, transactionId: id, amount: direction === 'DEBIT' ? '-10.00' : '10.00', currency: 'USD', date: '2026-07-11', transactionDate: '2026-07-11', direction, type: direction, sourceType: 'TRANSFER', description: `Transfer to ${accountNumber}`, reference: null }),
    })
    const source = [make('a', '22223333', 'DEBIT', 'a-tx'), make('b', '11112222', 'CREDIT', 'b-tx')]
    const correlated = correlateInvestecTransferObservations(source, { a: '11112222', b: '22223333' }, 'investec:principal')
    expect((source[0].payload as Record<string, unknown>).sourceGroupId).toBeUndefined()
    expect((correlated[0].payload as Record<string, unknown>).activityType).toBe('TRANSFER_OUT')
    expect((correlated[0].payload as Record<string, unknown>).sourceGroupId).toBe((correlated[1].payload as Record<string, unknown>).sourceGroupId)
    expect(() => finalizeInvestecObservations(correlated)).not.toThrow()
  })

  it('leaves same-amount own-account rows unpaired when reciprocal evidence is ambiguous', async () => {
    const accounts = [
      { accountId: 'acc-a', accountNumber: '10001111', productName: 'Private Bank Account' },
      { accountId: 'acc-b', accountNumber: '10002222', productName: 'Private Bank Account' },
      { accountId: 'acc-c', accountNumber: '10003333', productName: 'Private Bank Account' },
    ]
    const transport = makeTransport([], undefined, accounts)
    transport.getTransactionPage = async (accountId) => ({ transactions: accountId === 'acc-a' ? [{ uuid: 'out-1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: '2500.00', transactionDate: '2026-07-10', description: 'Transfer to 10002222 or 10003333' }] : accountId === 'acc-b' ? [{ uuid: 'in-1', status: 'POSTED', postedOrder: 1, type: 'CREDIT', amount: '2500.00', transactionDate: '2026-07-10', description: 'Transfer from 10001111' }] : [{ uuid: 'in-2', status: 'POSTED', postedOrder: 2, type: 'CREDIT', amount: '2500.00', transactionDate: '2026-07-10', description: 'Transfer from 10001111' }] })
    const result = await runInvestecAdapter({ ...BASE_CONFIG, accountIds: ['acc-a', 'acc-b', 'acc-c'] }, transport)
    expect(transactionObservations(result).every(item => item.payload.sourceGroupId === undefined)).toBe(true)
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

  it('marks conflicting lifecycle identifiers for review', async () => {
    const r = await runInvestecAdapter(BASE_CONFIG, makeTransport([{
      uuid: 'conflict-1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', transactionType: 'CARDPURCHASES',
      amount: '10.00', transactionDate: '2026-08-05', pendingTransactionId: 'card-1', postedTransactionId: 'card-2',
    }], undefined))
    const [observation] = transactionObservations(r)
    expect(observation.payload).toMatchObject({ needsReview: true, providerLifecycleIdConflict: true })
    expect(observation.payload.providerLifecycleTransactionId).toBeUndefined()
    expect(observation.payload.metadata).toMatchObject({
      providerOwned: { investec: { providerLifecycleIdConflict: true } },
    })
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
      actionDate: '2026-08-05', valueDate: '2026-08-06',
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
