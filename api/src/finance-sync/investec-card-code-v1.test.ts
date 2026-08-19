import { describe, expect, it } from 'vitest'
import { parseInvestecCardCodeEnvelope, parseInvestecCardDelivery, recoverInvestecCardExecution, sanitizeInvestecCardEnvelope } from './investec-card-code-v1'

describe('Investec Card Code v1 envelope', () => {
  it('accepts a complete after-transaction envelope and normalizes its provider time', () => {
    expect(parseInvestecCardCodeEnvelope({
      schemaVersion: 1,
      eventType: 'after_transaction',
      providerEventTime: '2026-08-14T17:00:00+00:00',
      transaction: { accountNumber: '1234', centsAmount: 100, currencyCode: 'USD' },
    })).toMatchObject({
      schemaVersion: 1,
      eventType: 'after_transaction',
      providerEventTime: '2026-08-14T17:00:00.000Z',
      transaction: { accountNumber: '1234' },
    })
  })

  it('wraps the direct transaction body emitted by the admitted queue producer', () => {
    expect(parseInvestecCardDelivery({
      accountNumber: '1234', centsAmount: 100, currencyCode: 'USD', type: 'card',
      dateTime: '2026-08-14T17:00:00+00:00', reference: 'direct-1',
    })).toMatchObject({
      schemaVersion: 1,
      eventType: 'after_transaction',
      providerEventTime: '2026-08-14T17:00:00.000Z',
      transaction: { accountNumber: '1234', centsAmount: 100 },
    })
    expect(parseInvestecCardDelivery({ accountNumber: '1234', centsAmount: 100, currencyCode: 'USD' })).toBeUndefined()
  })

  it('recovers only complete non-sandbox after-transaction execution evidence', () => {
    const recovered = recoverInvestecCardExecution({
      executionId: 'execution-1', sandbox: false, type: 'after_transaction', authorizationApproved: null,
      completedAt: '2026-08-14T17:16:41Z', logs: [{ content: JSON.stringify({
        accountNumber: '1234', centsAmount: 100, currencyCode: 'USD', dateTime: '2026-08-14T17:00:00Z',
      }) }],
    })
    expect(recovered?.eventType).toBe('after_transaction')
    expect(recovered?.transaction).toMatchObject({ centsAmount: 100 })
    expect(recoverInvestecCardExecution({
      executionId: 'sandbox', sandbox: true, type: 'after_transaction', completedAt: '2026-08-14T17:16:41Z', logs: [],
    })).toBeUndefined()
    expect(recoverInvestecCardExecution({
      executionId: 'incomplete', sandbox: false, type: 'after_transaction', completedAt: '2026-08-14T17:16:41Z', logs: [{ content: '{bad' }],
    })).toBeUndefined()
  })

  it('recovers complete evidence when webhook delivery failed, but rejects authorization failures', () => {
    const execution = {
      executionId: 'delivery-failed', sandbox: false, type: 'after_transaction', authorizationApproved: true,
      error: { message: 'webhook returned HTTP 503' }, completedAt: '2026-08-14T17:16:41Z',
      logs: [{ content: JSON.stringify({ accountNumber: '1234', centsAmount: 100, currencyCode: 'USD', dateTime: '2026-08-14T17:00:00Z' }) }],
    }
    expect(recoverInvestecCardExecution(execution)?.eventType).toBe('after_transaction')
    expect(recoverInvestecCardExecution({ ...execution, error: { message: 'card authorization declined' } })).toBeUndefined()
  })

  it('redacts PAN fields recursively without redacting the bank account number', () => {
    expect(sanitizeInvestecCardEnvelope({
      accountNumber: '10001111222',
      card: { number: 4111111111111111, display: '4111 1111 1111 1111' },
      cardNumber: '5555555555554444',
    })).toEqual({
      accountNumber: '10001111222',
      card: { number: '••••1111', display: '••••1111' },
      cardNumber: '••••4444',
    })
  })

  it('does not recover a declined envelope from an after-transaction execution row', () => {
    expect(recoverInvestecCardExecution({
      executionId: 'declined-log', sandbox: false, type: 'after_transaction', authorizationApproved: null,
      completedAt: '2026-08-14T17:16:41Z', logs: [{ content: JSON.stringify({
        schemaVersion: 1, eventType: 'after_decline', providerEventTime: '2026-08-14T17:00:00Z',
        transaction: { accountNumber: '1234', centsAmount: 100, currencyCode: 'USD', dateTime: '2026-08-14T17:00:00Z' },
      }) }],
    })).toBeUndefined()
  })

  it('rejects incomplete, malformed, or unsupported envelopes', () => {
    expect(parseInvestecCardCodeEnvelope({ schemaVersion: 1, eventType: 'after_transaction', transaction: {}, providerEventTime: 'bad' })).toBeUndefined()
    expect(parseInvestecCardCodeEnvelope({ schemaVersion: 1, eventType: 'after_transaction', providerEventTime: '2026-01-01T00:00:00Z' })).toBeUndefined()
    expect(parseInvestecCardCodeEnvelope({ schemaVersion: 2, eventType: 'after_transaction', transaction: {}, providerEventTime: '2026-01-01T00:00:00Z' })).toBeUndefined()
  })
})
