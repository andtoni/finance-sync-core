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
        amount: '-5.76', currency: 'ZAR', date: '2026-08-05',
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
  })

  it('does not create a zero-value card activity', () => {
    expect(investecCardObservation({
      ...input,
      payload: { accountNumber: '10000000001', centsAmount: 0, dateTime: '2026-08-05T13:40:00.000Z', merchant: { name: 'Visa Provisioning Service' } },
    })).toBeNull()
  })
})
