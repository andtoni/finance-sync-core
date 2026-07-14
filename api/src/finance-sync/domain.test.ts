import { describe, expect, it } from 'vitest'
import {
  assertCurrency,
  canonicalObservationIdentity,
  type CanonicalDomain,
  type CashLedgerObservation,
} from './domain'

describe('Finance Sync canonical domains', () => {
  it('keeps financial resource domains typed instead of using a universal transaction', () => {
    const domains: readonly CanonicalDomain[] = [
      'cash_ledger',
      'balance',
      'liability',
      'brokerage_activity',
      'holding',
      'security',
      'cost_basis',
      'order',
    ]

    expect(domains).toHaveLength(8)
    expect(domains).not.toContain('transaction')
  })

  it('rejects publication when account currency is not proven', () => {
    expect(() => assertCurrency(null)).toThrowError('MISSING_ACCOUNT_CURRENCY')
    expect(assertCurrency('zar')).toBe('ZAR')
  })

  it('builds observation identity from source incarnation, provider version, semantic hash and hash specification', () => {
    const observation: CashLedgerObservation = {
      deploymentId: 'dep-1',
      localOwnerId: 'owner-1',
      sourceResourceIncarnationId: 'src-1',
      providerVersion: null,
      semanticHash: 'sha256:abc',
      hashSpecificationVersion: 1,
      accountCurrency: 'ZAR',
      amount: '-12.34',
      effectiveAt: '2026-07-11T00:00:00.000Z',
    }

    expect(canonicalObservationIdentity(observation)).toEqual([
      'dep-1',
      'src-1',
      '',
      'sha256:abc',
      '1',
    ].join('\u001f'))
  })
})
