import { describe, expect, it } from 'vitest'
import { investecCardObservation } from './investec-card-drainer'

const input = {
  deploymentId: 'wealthfolio-production',
  localOwnerId: 'urn:wealthfolio:password#local',
  sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
  providerAccountId: '1000000000000000000000001',
  accountCurrency: 'ZAR',
}

describe('Investec card observations', () => {
  it('preserves a card authorization as an explicitly pending native cash fact', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '10000000001', centsAmount: 576, currencyCode: 'usd', type: 'card',
        dateTime: '2026-08-05T16:00:00.000Z', reference: '500000001',
        merchant: { name: 'SAASCO' }, card: { display: '400000XXXXXX0011' },
      },
    })

    expect(observation).toMatchObject({
      providerVersion: 'investec:card:investec-card:1000000000000000000000001:reference-500000001',
      observedAt: '2026-08-05T16:00:00.000Z',
      payload: {
        transactionId: 'investec-card:1000000000000000000000001:reference-500000001',
        amount: '-5.76', currency: 'ZAR', originalCurrency: 'USD',
        settlementCurrency: 'ZAR', date: '2026-08-05', transactionDate: '2026-08-05',
        description: 'SAASCO', status: 'pending', reference: '500000001',
      },
    })
    expect(observation?.payload.merchantContext).toMatchObject({
      merchantDisplay: 'SAASCO', direction: 'outflow',
      providerHint: { reference: '500000001' },
    })
  })

  it('is deterministic for webhook redelivery and ignores the merchant currency leg', () => {
    const payload = {
      accountNumber: '10000000001', centsAmount: 576, currencyCode: 'usd', type: 'card',
      dateTime: '2026-08-05T16:00:00.000Z', reference: '500000001', merchant: { name: 'SAASCO' },
    }
    const first = investecCardObservation({ ...input, payload })
    const second = investecCardObservation({ ...input, payload })
    expect(first?.semanticHash).toBe(second?.semanticHash)
    expect(first?.payload.currency).toBe('ZAR')
    expect(first?.payload.originalAmount).toBeUndefined()
    expect(first?.payload.originalCurrency).toBe('USD')
    expect(first?.payload.settlementCurrency).toBe('ZAR')
    expect(first?.hashSpecificationVersion).toBe(3)
  })

  it('hashes a newly supplied provider authorization time even with a stable reference', () => {
    const base = {
      ...input,
      payload: {
        accountNumber: '10000000001', centsAmount: 576, currencyCode: 'usd', type: 'card',
        reference: '500000001', merchant: { name: 'SAASCO' },
      },
      receivedAt: '2026-08-05T16:00:00.000Z',
    }
    const withoutProviderTime = investecCardObservation(base)
    const withProviderTime = investecCardObservation({
      ...base,
      payload: { ...base.payload, dateTime: '2026-08-05T16:07:01.000Z' },
    })
    expect(withProviderTime?.semanticHash).not.toBe(withoutProviderTime?.semanticHash)
  })

  it('keeps card credits positive even when the webhook uses centsAmount', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        centsAmount: 1250, currencyCode: 'usd', type: 'CREDIT',
        dateTime: '2026-08-05T16:08:00.000Z', reference: 'refund-1',
        merchant: { name: 'SAASCO REFUND' },
      },
    })
    expect(observation?.payload.amount).toBe('12.50')
    expect(observation?.payload.currency).toBe('ZAR')
    expect(observation?.payload.type).toBe('CREDIT')
  })

  it('does not create a zero-value card activity', () => {
    expect(investecCardObservation({
      ...input,
      payload: { accountNumber: '10000000001', centsAmount: 0, dateTime: '2026-08-05T13:40:00.000Z', merchant: { name: 'Visa Provisioning Service' } },
    })).toBeNull()
  })
})
