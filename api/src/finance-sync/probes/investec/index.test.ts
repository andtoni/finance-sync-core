import { describe, expect, it, vi } from 'vitest'
import { runInvestecG0Probe, type InvestecProbeTransport } from './index'

const transport = (overrides: Partial<InvestecProbeTransport> = {}): InvestecProbeTransport => ({
  listAccounts: async () => [
    { accountId: 'cash-123456', productName: 'Private Bank Account' },
    { accountId: 'mortgage-99', productName: 'Home Loan' },
  ],
  getBalance: async () => ({ currency: 'ZAR' }),
  getTransactionPage: async (_id, _window, cursor) => cursor
    ? { transactions: [{ uuid: 'booked-2', status: 'POSTED', postedOrder: 2, type: 'CREDIT', amount: 3, postingDate: '2026-07-10' }] }
    : { transactions: [
      { uuid: 'booked-1', status: 'POSTED', postedOrder: 1, type: 'DEBIT', amount: 12, postingDate: '2026-07-09' },
      { uuid: 'provisional-secret', status: 'POSTED', postedOrder: 0, type: 'DEBIT', amount: 4, postingDate: '2026-07-10' },
      { uuid: 'pending-secret', status: 'PENDING', postedOrder: 3, type: 'DEBIT', amount: 5, postingDate: '2026-07-10' },
    ], nextCursor: 'opaque-secret' },
  ...overrides,
})

describe('disposable Investec G0 synthetic probe', () => {
  it('proves only bounded eligible cash accounts over a booked-only paged window', async () => {
    const result = await runInvestecG0Probe(transport(), {
      fromDate: '2026-07-01', toDate: '2026-07-11', eligibleProductNames: ['Private Bank Account'], maxPages: 3,
    })

    expect(result.capability).toBe('supported_with_quarantine')
    expect(result.projectionEnabled).toBe(false)
    expect(result.accounts).toEqual([{ accountRef: '…3456', eligible: true, currency: 'ZAR' }])
    expect(result.booked).toEqual([
      expect.objectContaining({ accountRef: '…3456', amount: '-12.00', currency: 'ZAR' }),
      expect.objectContaining({ accountRef: '…3456', amount: '3.00', currency: 'ZAR' }),
    ])
    expect(result.quarantine).toEqual([
      expect.objectContaining({ reason: 'POSTED_ORDER_ZERO_PROVISIONAL', accountRef: '…3456' }),
    ])
    expect(result.evidence.window).toEqual({ fromDate: '2026-07-01', toDate: '2026-07-11' })
    expect(result.evidence.pages).toBe(2)
    expect(result.evidence).toMatchObject({ bookedOnly: true, accountCurrencyProven: true })
    expect(JSON.stringify(result)).not.toMatch(/123456|mortgage-99|secret/)
    expect(result.uncertainty).toEqual(expect.arrayContaining(['SIGN_SEMANTICS', 'DATE_SEMANTICS', 'IDENTITY_STABILITY']))
  })

  it('fails closed when currency is absent and records bounded pagination uncertainty', async () => {
    const getTransactionPage = vi.fn(async () => ({ transactions: [], nextCursor: 'again' }))
    const result = await runInvestecG0Probe(transport({
      listAccounts: async () => [
        { accountId: 'missing-currency', productName: 'Private Bank Account' },
        { accountId: 'paged-account', productName: 'Private Bank Account' },
      ],
      getBalance: async (id) => id === 'missing-currency' ? {} : { currency: 'ZAR' },
      getTransactionPage,
    }), {
      fromDate: '2026-07-01', toDate: '2026-07-11', eligibleProductNames: ['Private Bank Account'], maxPages: 1,
    })
    expect(result.capability).toBe('unsupported')
    expect(result.booked).toEqual([])
    expect(result.evidence).toMatchObject({ pages: 1, paginationComplete: false, accountCurrencyProven: false })
    expect(result.quarantine).toContainEqual(expect.objectContaining({ reason: 'MISSING_ACCOUNT_CURRENCY' }))
    expect(result.uncertainty).toContain('PAGINATION_COMPLETENESS')
  })

  it('requires injected transport and accepts no credentials or scheduling/projection controls', () => {
    expect(runInvestecG0Probe.length).toBe(2)
  })
})
