import { describe, expect, it } from 'vitest'
import { runSnapTradeG0Probe, type SnapTradeProbeTransport } from './index'

describe('SnapTrade G0 synthetic probe', () => {
  it('detects offset boundary mutation and reports source freshness without activating partial holdings', async () => {
    let boundaryRead = 0
    const transport: SnapTradeProbeTransport = {
      fetchHoldingsPage: async (offset, limit) => {
        expect(limit).toBe(2)
        if (offset === 0) return { holdings: [{ id: 'holding-secret-a', asOf: '2026-07-10T12:00:00Z' }, { id: 'holding-secret-b', asOf: '2026-07-10T12:00:00Z' }], hasMore: true, complete: true }
        boundaryRead++
        return { holdings: [{ id: boundaryRead === 1 ? 'holding-secret-b' : 'holding-secret-x', asOf: '2026-07-10T12:00:00Z' }], hasMore: false, complete: true }
      },
    }
    const result = await runSnapTradeG0Probe(transport, { pageSize: 2, observedAt: '2026-07-11T12:00:00Z' })
    expect(result.outcome).toBe('unstable')
    expect(result.projectionEnabled).toBe(false)
    expect(result.activationAllowed).toBe(false)
    expect(result.evidence).toMatchObject({ pagination: 'offset', boundaryStable: false, sourceAsOf: '2026-07-10T12:00:00.000Z', freshnessSeconds: 86400, holdingsComplete: true })
    expect(JSON.stringify(result)).not.toContain('secret')
  })

  it('fails closed and never activates an incomplete holdings payload', async () => {
    const partial: SnapTradeProbeTransport = { fetchHoldingsPage: async () => ({ holdings: [{ id: 'private', asOf: null }], hasMore: false, complete: false }) }
    const result = await runSnapTradeG0Probe(partial, { pageSize: 10, observedAt: '2026-07-11T12:00:00Z' })
    expect(result.outcome).toBe('partial')
    expect(result.activationAllowed).toBe(false)
    expect(result.evidence.holdingsComplete).toBe(false)

    const broken: SnapTradeProbeTransport = { fetchHoldingsPage: async () => { throw new Error('consumer-key') } }
    expect(JSON.stringify(await runSnapTradeG0Probe(broken))).not.toContain('consumer-key')
  })
})
