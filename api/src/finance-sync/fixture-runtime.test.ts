import { describe, expect, it } from 'vitest'
import { createFixtureRuntime } from './fixture-runtime'

describe('fixture provider runtime', () => {
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
