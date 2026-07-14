import { describe, expect, it } from 'vitest'
import { runMercuryG0Probe, type MercuryProbeTransport } from './index'

describe('Mercury G0 synthetic probe', () => {
  it('follows one organisation-wide cursor and records uncertain native semantics without projection', async () => {
    const calls: Array<string | null> = []
    const transport: MercuryProbeTransport = {
      fetchOrganisationPage: async (cursor) => {
        calls.push(cursor)
        return cursor === null
          ? { items: [{ id: 'txn-secret-1', accountId: 'acct-secret-a', status: 'mystery', relatedTransactionId: null }], nextCursor: 'cursor-secret' }
          : { items: [{ id: 'txn-secret-2', accountId: 'acct-secret-b', status: 'sent', relatedTransactionId: 'txn-secret-1' }], nextCursor: null }
      },
      refetchTransaction: async () => ({ id: 'txn-secret-1', accountId: 'acct-secret-a', status: 'sent', relatedTransactionId: null }),
    }

    const result = await runMercuryG0Probe(transport, { webhook: { transactionId: 'txn-secret-1', payload: { token: 'do-not-leak' } } })

    expect(calls).toEqual([null, 'cursor-secret'])
    expect(result.outcome).toBe('observed')
    expect(result.projectionEnabled).toBe(false)
    expect(result.activationAllowed).toBe(false)
    expect(result.evidence).toMatchObject({ cursorScope: 'organisation', nativeStateCertain: false, relationshipCertain: false, webhookUse: 'wake-and-refetch' })
    expect(JSON.stringify(result)).not.toContain('secret')
  })

  it('fails closed when cursor progression loops or transport fails', async () => {
    const looping: MercuryProbeTransport = { fetchOrganisationPage: async () => ({ items: [], nextCursor: 'same' }), refetchTransaction: async () => { throw new Error('credential leaked') } }
    expect((await runMercuryG0Probe(looping)).outcome).toBe('failed_closed')
    const broken: MercuryProbeTransport = { fetchOrganisationPage: async () => { throw new Error('Bearer live-secret') }, refetchTransaction: async () => { throw new Error() } }
    const result = await runMercuryG0Probe(broken)
    expect(result.outcome).toBe('failed_closed')
    expect(JSON.stringify(result)).not.toContain('live-secret')
  })
})
