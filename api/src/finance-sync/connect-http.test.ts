import { createServer } from 'node:http'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { FinanceSyncConnectControl, wealthfolioEmptyAccountAttestation, wealthfolioProviderRelinkAttestation } from './connect-control'
import { createFinanceSyncHttpHandler } from './connect-http'

const controlToken = 'dedicated-control-token'
const subject = 'wealthfolio#owner-a'
let server: ReturnType<typeof createServer>
let base: string
let admitted: Record<string, unknown>[]

beforeEach(async () => {
  admitted = []
  const connect = new FinanceSyncConnectControl({
    deploymentId: 'deployment-1',
    connections: [],
    plaid: {
      createLinkToken: async () => ({ linkToken: 'link-token' }),
      exchangePublicToken: async () => ({ clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'durable-secret', itemId: 'item-1' }),
      listAccounts: async () => [{ providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', assets: [] }],
    },
    secretStore: {
      available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'],
      storePlaidAccessToken: async () => 'PLAID_SLOT_A',
    },
    sessionStore: { create: async () => 'continuation-token', consume: async () => 'PLAID_SLOT_A', release: async () => undefined },
    onboarding: {
      listPending: async boundSubject => [{ connectionId: 'connection-1', providerGeneration: 3, providerAccountId: 'account-1', name: boundSubject, type: 'checking', currency: 'USD', assets: [] }],
      admit: async input => {
        admitted.push(input)
        return { wealthfolioAccountId: input.wealthfolioAccountId, providerGeneration: input.providerGeneration,
          receiverAccessGeneration: input.receiverAccessGeneration, baselineStatus: 'verified',
          baselineDigest: input.databaseStateDigest, assetMappingDigest: 'sha256:empty' }
      },
      revoke: async (boundSubject, provider, connectionId) => boundSubject === subject && provider === 'plaid' && connectionId === 'connection-1',
      reconciliationStatus: async () => ({ wealthfolioAccountId: 'wf-1', providerGeneration: 3,
        receiverAccessGeneration: 7, baselineStatus: 'verified', baselineDigest: 'sha256:baseline', assetMappingDigest: 'sha256:empty' }),
    },
    onPlaidConnected: async () => undefined,
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
    expect(payload).toEqual({ provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' })
    expect(JSON.stringify(payload)).not.toContain('durable-secret')
  })

  it('binds account discovery and admission to the authenticated subject', async () => {
    const pending = await fetch(`${base}/control/v1/provider-connections/plaid/accounts`, { headers: headers() })
    expect(pending.status).toBe(200)
    expect(await pending.json()).toEqual([{ connectionId: 'connection-1', providerGeneration: 3, providerAccountId: 'account-1', name: subject, type: 'checking', currency: 'USD', assets: [] }])

    const request = { authenticatedSubject: subject, connectionId: 'connection-1', providerAccountId: 'account-1', wealthfolioAccountId: 'wf-1', providerGeneration: 3, receiverAccessGeneration: 7, baselineKind: 'empty' as const, assetMappings: [] }
    const databaseStateDigest = wealthfolioEmptyAccountAttestation({ ...request, provider: 'plaid' })
    // Shared Wealthfolio Rust/Finance TypeScript fixture vector (64-bit big-endian length framing).
    expect(databaseStateDigest).toMatch(/^sha256:[a-f0-9]{64}$/)
    const admission = await fetch(`${base}/control/v1/provider-connections/plaid/admit`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ ...request, databaseStateDigest }),
    })
    expect(admission.status).toBe(200)
    expect(admitted).toEqual([{ subject, provider: 'plaid', connectionId: 'connection-1', providerAccountId: 'account-1', wealthfolioAccountId: 'wf-1', databaseStateDigest, providerGeneration: 3, receiverAccessGeneration: 7, baselineKind: 'empty', assetMappings: [] }])

    // A lost admission response is reconciled by the exact authenticated key,
    // without requiring the account to remain in pending discovery.
    const recovered = await fetch(`${base}/control/v1/provider-connections/plaid/admissions/connection-1/account-1`, { headers: headers() })
    expect(recovered.status).toBe(200)
    expect(await recovered.json()).toEqual({ wealthfolioAccountId: 'wf-1', providerGeneration: 3,
      receiverAccessGeneration: 7, baselineStatus: 'verified', baselineDigest: 'sha256:baseline', assetMappingDigest: 'sha256:empty' })

    const forged = await fetch(`${base}/control/v1/provider-connections/plaid/admit`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }), body: JSON.stringify({ ...request, databaseStateDigest: `${databaseStateDigest}evil` }),
    })
    expect(forged.status).toBe(409)

    const relinkRequest = { ...request, baselineKind: 'relink' as const, priorProviderGeneration: 2 }
    const relinkDigest = wealthfolioProviderRelinkAttestation({ ...relinkRequest, provider: 'plaid' })
    const relink = await fetch(`${base}/control/v1/provider-connections/plaid/admit`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ ...relinkRequest, databaseStateDigest: relinkDigest }),
    })
    expect(relink.status).toBe(200)
    expect(admitted.at(-1)).toMatchObject({ subject, provider: 'plaid', providerGeneration: 3,
      baselineKind: 'relink', priorProviderGeneration: 2, databaseStateDigest: relinkDigest })
  })

  it('disconnects a subject-bound provider without exposing a 501 path', async () => {
    const response = await fetch(`${base}/control/v1/provider-connections/plaid/connection-1`, { method: 'DELETE', headers: headers() })
    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({ provider: 'plaid', status: 'disconnected', message: 'Provider connection was disconnected.' })
    const absent = await fetch(`${base}/control/v1/provider-connections/mercury/connection-1`, { method: 'DELETE', headers: headers() })
    expect(absent.status).toBe(404)
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
