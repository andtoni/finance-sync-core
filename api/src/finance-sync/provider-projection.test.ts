import { describe, expect, it } from 'vitest'
import { providerObservationToBridge } from './projection/bridge-client'

const observation = (payload: Record<string, unknown>, domain = 'cash_ledger') => ({
  deploymentId: 'dep-1', localOwnerId: 'urn:test#owner', sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
  semanticHash: 'a'.repeat(64), domain, payload, observedAt: '2026-07-18T00:00:00.000Z',
})

describe('provider-aware correction and removal projection', () => {
  it('normalizes direct-bank ledger signs without applying Plaid signs', () => {
    const debit = providerObservationToBridge('mercury', observation({ transactionId: 't-1', accountId: 'a', amount: '-12.50', currency: 'USD', date: '2026-07-18' }), 1, 'wf-a')
    const credit = providerObservationToBridge('investec', observation({ transactionId: 't-2', accountId: 'a', amount: '20.00', currency: 'ZAR', date: '2026-07-18' }), 1, 'wf-a')
    expect(debit?.activity).toMatchObject({ activityType: 'WITHDRAWAL', amount: '12.5' })
    expect(credit?.activity).toMatchObject({ activityType: 'DEPOSIT', amount: '20' })
  })

  it('maps SnapTrade activity kinds while leaving holdings canonical-only', () => {
    const activity = providerObservationToBridge('snaptrade', observation({ sourceId: 'snaptrade:a-1', accountId: 'a', type: 'buy', amount: '42.00', currency: 'USD', date: '2026-07-18' }, 'brokerage_activity'), 1, 'wf-a')
    expect(activity?.activity).toMatchObject({ activityType: 'BUY', amount: '42', status: 'POSTED' })
    expect(providerObservationToBridge('snaptrade', observation({ accountId: 'a', symbol: 'VTI', units: '1' }, 'holding'), 1, 'wf-a')).toBeNull()
  })

  it('retains unresolved removals as canonical evidence without visible deletion', () => {
    const removed = providerObservationToBridge('plaid', observation({ transactionId: 't-1', isRemoved: true }), 1, 'wf-a')
    const corrected = providerObservationToBridge('plaid', observation({ transactionId: 't-1', amount: 5, currency: 'USD', date: '2026-07-18' }), 1, 'wf-a')
    expect(removed).toBeNull()
    expect(corrected?.kind).toBe('ActivityUpsert')
  })
})
