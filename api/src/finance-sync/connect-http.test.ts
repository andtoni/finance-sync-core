import { createServer } from 'node:http'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { FinanceSyncConnectControl } from './connect-control'
import { createFinanceSyncHttpHandler } from './connect-http'

const controlToken = 'dedicated-control-token'
const subject = 'wealthfolio#owner-a'
let server: ReturnType<typeof createServer>
let base: string

beforeEach(async () => {
  const connect = new FinanceSyncConnectControl({
    deploymentId: 'deployment-1',
    connections: [],
    plaid: {
      createLinkToken: async () => ({ linkToken: 'link-token' }),
      exchangePublicToken: async () => ({ accessToken: 'durable-secret', itemId: 'item-1' }),
    },
    secretStore: { available: true, storePlaidAccessToken: async () => undefined },
    sessionStore: { create: async () => 'continuation-token', consume: async () => true },
  })
  const handler = createFinanceSyncHttpHandler({ controlToken, connect, health: () => ({ status: 200, body: { ok: true } }) })
  server = createServer((request, response) => { void handler(request, response) })
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve) })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('test server did not bind')
  base = `http://127.0.0.1:${address.port}`
})

afterEach(async () => new Promise<void>(resolve => server.close(() => resolve())))

function headers(extra: Record<string, string> = {}): Record<string, string> {
  return { authorization: `Bearer ${controlToken}`, 'x-authenticated-subject': subject, ...extra }
}

describe('Finance Sync internal Connect HTTP routes', () => {
  it('authenticates the shared list and start routes with the dedicated bearer and subject headers', async () => {
    expect((await fetch(`${base}/control/v1/provider-connections`)).status).toBe(401)
    expect((await fetch(`${base}/control/v1/provider-connections`, { headers: { authorization: 'Bearer wrong', 'x-authenticated-subject': subject } })).status).toBe(401)
    const listed = await fetch(`${base}/control/v1/provider-connections`, { headers: headers() })
    expect(listed.status).toBe(200)
    expect(await listed.json()).toHaveLength(4)

    const started = await fetch(`${base}/control/v1/provider-connections/plaid/start`, { method: 'POST', headers: headers() })
    expect(started.status).toBe(200)
    expect(await started.json()).toMatchObject({ provider: 'plaid', status: 'link_ready', message: 'Plaid Link is ready.', linkToken: 'link-token', continuationToken: 'continuation-token' })
  })

  it('requires bounded JSON for Plaid completion and returns only a redacted typed response', async () => {
    const wrongType = await fetch(`${base}/control/v1/provider-connections/plaid/complete`, { method: 'POST', headers: headers(), body: '{}' })
    expect(wrongType.status).toBe(415)
    expect(await wrongType.json()).toEqual({ error: 'request_failed' })

    const oversized = await fetch(`${base}/control/v1/provider-connections/plaid/complete`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }), body: JSON.stringify({ publicToken: 'x'.repeat(17 * 1024) }),
    })
    expect(oversized.status).toBe(413)
    expect(await oversized.json()).toEqual({ error: 'request_failed' })

    const complete = await fetch(`${base}/control/v1/provider-connections/plaid/complete`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }), body: JSON.stringify({ publicToken: 'public-token', continuationToken: 'continuation-token' }),
    })
    expect(complete.status).toBe(200)
    const payload = await complete.json()
    expect(payload).toEqual({ provider: 'plaid', status: 'configured', message: 'Plaid connection was stored.' })
    expect(JSON.stringify(payload)).not.toContain('durable-secret')
  })

  it('does not accept a subject from query or JSON', async () => {
    const query = await fetch(`${base}/control/v1/provider-connections?subject=${encodeURIComponent(subject)}`, { headers: { authorization: `Bearer ${controlToken}` } })
    expect(query.status).toBe(400)
    const body = await fetch(`${base}/control/v1/provider-connections/plaid/complete`, {
      method: 'POST', headers: { authorization: `Bearer ${controlToken}`, 'content-type': 'application/json' },
      body: JSON.stringify({ subject, publicToken: 'public-token', continuationToken: 'continuation-token' }),
    })
    expect(body.status).toBe(400)
  })
})
