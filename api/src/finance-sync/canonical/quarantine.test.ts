import { describe, expect, it } from 'vitest'
import { qualifyCashObservation } from './quarantine'

describe('canonical publication quarantine', () => {
  it('quarantines economic values without proven account currency', () => {
    expect(qualifyCashObservation({ amount: '-12.34', accountCurrency: null })).toEqual({
      publishable: false,
      reason: 'MISSING_ACCOUNT_CURRENCY',
    })
  })

  it('rejects malformed amounts rather than coercing them', () => {
    expect(() => qualifyCashObservation({ amount: '12.', accountCurrency: 'USD' })).toThrow('amount')
    expect(() => qualifyCashObservation({ amount: 'NaN', accountCurrency: 'USD' })).toThrow('amount')
  })

  it('publishes exact decimal strings with proven currency', () => {
    expect(qualifyCashObservation({ amount: '-12.34', accountCurrency: 'USD' })).toEqual({
      publishable: true,
      money: { amount: '-12.34', currency: 'USD' },
    })
  })
})
