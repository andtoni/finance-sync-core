import { describe, expect, it } from 'vitest'
import { economicMoney, provenIsoCurrency } from './money'

describe('Finance Sync economic money', () => {
  it('requires an ISO 4217 currency with positive provenance', () => {
    const currency = provenIsoCurrency({ code: 'zar', kind: 'iso_4217', provenance: 'account_metadata' })
    expect(economicMoney('-12.3400', currency)).toEqual({ amount: '-12.3400', currency })
    expect(Object.isFrozen(currency)).toBe(true)
  })

  it('fails closed for unofficial, unknown, absent or malformed currency evidence', () => {
    expect(() => provenIsoCurrency({ code: 'USD', kind: 'provider_unofficial', provenance: 'payload' })).toThrowError('UNPROVEN_ISO_CURRENCY')
    expect(() => provenIsoCurrency({ code: 'USD', kind: 'iso_4217', provenance: 'absent' })).toThrowError('UNPROVEN_ISO_CURRENCY')
    expect(() => provenIsoCurrency({ code: 'ZZZ', kind: 'iso_4217', provenance: 'contractual' })).toThrowError('INVALID_ISO_CURRENCY')
  })

  it.each(['1e3', 'NaN', ' 12.3', '12.', '+1', ''])('rejects a non-canonical decimal amount %j', amount => {
    const currency = provenIsoCurrency({ code: 'GBP', kind: 'iso_4217', provenance: 'contractual' })
    expect(() => economicMoney(amount, currency)).toThrowError('INVALID_ECONOMIC_AMOUNT')
  })
})
