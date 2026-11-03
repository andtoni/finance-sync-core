import { describe, expect, it } from 'vitest'
import { startDrainer, type DrainerHandle } from '../drain/drainer'
import { financeSyncHealthResponse, webhookDrainState, type FinanceSyncHealth } from './health'

describe('webhook drain health', () => {
  it('goes unhealthy after 20 consecutive failed drain passes and recovers after a successful one', async () => {
    const failingPulls = 20
    let pulls = 0
    const fetchImpl = (async () => {
      pulls++
      return pulls <= failingPulls
        ? new Response('{}', { status: 500 })
        : new Response(JSON.stringify({ result: { messages: [] } }), { status: 200 })
    }) as unknown as typeof fetch

    const healthAtSleep: Array<{ status: number; body: FinanceSyncHealth }> = []
    const handle: DrainerHandle = startDrainer(
      { accountId: 'a', queueId: 'q', pullToken: 't' },
      {
        fetchImpl,
        handle: () => true,
        random: () => 0,
        sleep: async () => {
          healthAtSleep.push(financeSyncHealthResponse(true, true, 1, false, webhookDrainState(handle)))
          if (healthAtSleep.length === failingPulls + 1) handle.stop()
        },
      },
    )
    await handle.done

    expect(healthAtSleep[18]).toMatchObject({ status: 200, body: { ok: true, webhookDrain: 'ok' } })
    expect(healthAtSleep[19]).toMatchObject({ status: 503, body: { ok: false, webhookDrain: 'failing' } })
    expect(healthAtSleep[20]).toMatchObject({ status: 200, body: { ok: true, webhookDrain: 'ok' } })
  })

  it('treats a drainer that is not configured as disabled and healthy', () => {
    expect(webhookDrainState(undefined)).toBe('disabled')
    expect(financeSyncHealthResponse(true, true, 1, false, webhookDrainState(undefined)))
      .toMatchObject({ status: 200, body: { ok: true, webhookDrain: 'disabled' } })
  })
})
