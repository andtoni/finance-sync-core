import { describe, expect, it } from 'vitest'
import { addAuthorityEpoch, selectAuthority } from './epochs'

describe('authority epochs', () => {
  it('rejects overlapping publishing authority for an account/domain', () => {
    const first = addAuthorityEpoch([], {
      account: 'account-a', domain: 'cash_ledger', sourceAccount: 'source-a',
      role: 'primary', validFrom: '2026-01-01T00:00:00.000Z', validTo: null,
    })
    expect(() => addAuthorityEpoch(first, {
      account: 'account-a', domain: 'cash_ledger', sourceAccount: 'source-b',
      role: 'primary', validFrom: '2026-06-01T00:00:00.000Z', validTo: null,
    })).toThrow('overlap')
  })

  it('selects by account, domain and valid time without automatic failover', () => {
    const epochs = addAuthorityEpoch([], {
      account: 'account-a', domain: 'balance', sourceAccount: 'source-a',
      role: 'primary', validFrom: '2026-01-01T00:00:00.000Z', validTo: '2026-06-01T00:00:00.000Z',
    })
    const cutover = addAuthorityEpoch(epochs, {
      account: 'account-a', domain: 'balance', sourceAccount: 'source-b',
      role: 'primary', validFrom: '2026-06-01T00:00:00.000Z', validTo: null,
    })

    expect(selectAuthority(cutover, 'account-a', 'balance', '2026-05-01T00:00:00.000Z')?.sourceAccount).toBe('source-a')
    expect(selectAuthority(cutover, 'account-a', 'balance', '2026-07-01T00:00:00.000Z')?.sourceAccount).toBe('source-b')
    expect(selectAuthority(cutover, 'account-a', 'holding', '2026-07-01T00:00:00.000Z')).toBeNull()
  })
})
