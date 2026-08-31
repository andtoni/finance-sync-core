import { describe, expect, it } from 'vitest'
import { createFixtureRuntime } from './fixture-runtime'
import { runInvestecAdapter } from './probes/investec/investec-adapter'

describe('fixture provider runtime', () => {
  it('emits synthetic Investec equal and unequal card lifecycle fixtures', async () => {
    const runtime = createFixtureRuntime('fixture-deployment')
    const connection = runtime.connections.find(item => item.provider === 'investec')
    if (!connection || connection.provider !== 'investec') throw new Error('missing Investec fixture')
    expect(connection.sourceResourceIncarnationId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    )
    const result = await runInvestecAdapter({
      ...connection,
      providerConnectionId: 'fixture-investec',
      toDate: '2026-08-31',
    }, runtime.factories.investec(connection))
    const cards = result.observations.filter(observation => observation.providerVersion.startsWith('investec:card:'))
    const posted = result.observations.filter(observation => observation.payload.transactionType === 'CARDPURCHASES')
    expect(cards).toHaveLength(3)
    expect(posted).toHaveLength(4)
    expect(cards.some(observation => observation.payload.originalAmount === '1357.38')).toBe(true)
    expect(posted.some(observation => observation.payload.amount === '-1345.13')).toBe(true)
    expect(posted.some(observation => observation.payload.amount === '-123.45')).toBe(true)
  })

  it('keeps SnapTrade activities scoped to admitted accounts', async () => {
    const runtime = createFixtureRuntime('fixture-deployment')
    const connection = runtime.connections.find(item => item.provider === 'snaptrade')
    if (!connection || connection.provider !== 'snaptrade') throw new Error('missing SnapTrade fixture')
    const transport = runtime.factories.snaptrade(connection)

    await expect(transport.fetchActivities('2026-08-01', '2026-08-04', [])).rejects
      .toThrow('require admitted account IDs')
    await expect(transport.fetchActivities('2026-08-01', '2026-08-04', ['other-account'])).resolves
      .toEqual([])
    await expect(transport.fetchActivities('2026-08-01', '2026-08-04', ['fixture-alpaca'])).resolves
      .toEqual([expect.objectContaining({ account: { id: 'fixture-alpaca' } })])
  })
})
