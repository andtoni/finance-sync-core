import { createServer } from 'node:http'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { FinanceSyncConnectControl, wealthfolioEmptyAccountAttestation, wealthfolioProviderRelinkAttestation } from './connect-control'
import { createFinanceSyncHttpHandler } from './connect-http'

const controlToken = 'dedicated-control-token'
const subject = 'wealthfolio#owner-a'
let server: ReturnType<typeof createServer>
let base: string
let admitted: Record<string, unknown>[]
let quarantined: Record<string, unknown>[]
let corrected: Record<string, unknown>[]

beforeEach(async () => {
  admitted = []
  quarantined = []
  corrected = []
  const connect = new FinanceSyncConnectControl({
    deploymentId: 'deployment-1',
    connections: [{
      provider: 'plaid' as const, connectionId: 'runtime-connection', providerConnectionId: 'item-1', deploymentId: 'deployment-1',
      localOwnerId: subject, sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
      accountIds: ['account-1'], wealthfolioAccountIds: { 'account-1': 'wf-1' },
      providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'PLAID_SLOT_A',
      clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'durable-secret',
    }],
    plaid: {
      createLinkToken: async () => ({ linkToken: 'link-token' }),
      createUpdateLinkToken: async () => ({ linkToken: 'update-link-token' }),
      exchangePublicToken: async () => ({ clientId: 'client-id', clientSecret: 'client-secret', accessToken: 'durable-secret', itemId: 'item-1' }),
      listAccounts: async () => [{ providerAccountId: 'account-1', name: 'Checking', type: 'checking', currency: 'USD', assets: [] }],
    },
    secretStore: {
      available: true, allowedSlots: ['PLAID_SLOT_A', 'PLAID_SLOT_B'],
      storePlaidAccessToken: async () => 'PLAID_SLOT_A',
    },
    sessionStore: {
      create: async () => 'continuation-token',
      consume: async input => input.flow === 'update'
        ? { secretReference: 'PLAID_SLOT_A', flow: 'update' as const, providerConnectionId: 'item-1', providerGeneration: 1 }
        : { secretReference: 'PLAID_SLOT_A', flow: 'new' as const },
      release: async () => undefined,
    },
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
    onPlaidAccountsUpdated: async () => undefined,
  })
  const handler = createFinanceSyncHttpHandler({
    controlToken, providersEnabled: true, connect,
    quarantine: {
      create: async (rawSubject, request) => {
        const result = { rawSubject, ...request, quarantineId: 'quarantine-1' }
        quarantined.push(result)
        return result
      },
    },
    investecCardCorrection: {
      create: async (rawSubject, request) => {
        const result = { rawSubject, ...request, correctionId: 'correction-1' }
        corrected.push(result)
        return result
      },
    },
    health: () => ({ status: 200, body: { ok: true } }),
  })
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

    const update = await fetch(`${base}/control/v1/provider-connections/plaid/update`, { method: 'POST', headers: headers() })
    expect(update.status).toBe(200)
    expect(await update.json()).toMatchObject({ provider: 'plaid', status: 'link_ready', message: 'Plaid Link is ready to add accounts.', linkToken: 'update-link-token', continuationToken: 'continuation-token' })

    const updateComplete = await fetch(`${base}/control/v1/provider-connections/plaid/update/complete`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }), body: JSON.stringify({ continuationToken: 'continuation-token' }),
    })
    expect(updateComplete.status).toBe(200)
    expect(await updateComplete.json()).toEqual({ provider: 'plaid', status: 'pending', message: 'Choose the Plaid accounts to add.' })
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

  it('creates an owner-authorized Investec card currency correction request', async () => {
    const request = {
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
      providerVersion: 'investec:card:investec-card:account:reference-1',
      semanticHash: 'a'.repeat(64), hashSpecificationVersion: 1,
      amount: '1000.00', currency: 'USD', reason: 'owner authorized USD card evidence',
    }
    const response = await fetch(`${base}/control/v1/investec/card-currency-corrections`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }), body: JSON.stringify(request),
    })
    expect(response.status).toBe(200)
    expect(await response.json()).toMatchObject({ ...request, rawSubject: subject, correctionId: 'correction-1' })
    expect(corrected).toEqual([{ ...request, rawSubject: subject, correctionId: 'correction-1' }])

    const invalid = await fetch(`${base}/control/v1/investec/card-currency-corrections`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ ...request, amount: '0.00' }),
    })
    expect(invalid.status).toBe(400)
  })

  it('creates an exact authenticated observation quarantine request', async () => {
    const request = {
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
      providerVersion: 'investec:txn:synthetic-1',
      semanticHash: 'a'.repeat(64), hashSpecificationVersion: 3,
      reason: 'exact synthetic observation correction',
    }
    const response = await fetch(`${base}/control/v1/observation-quarantines`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }), body: JSON.stringify(request),
    })
    expect(response.status).toBe(200)
    expect(await response.json()).toMatchObject({ ...request, rawSubject: subject, quarantineId: 'quarantine-1' })
    expect(quarantined).toEqual([{ ...request, rawSubject: subject, quarantineId: 'quarantine-1' }])

    const invalid = await fetch(`${base}/control/v1/observation-quarantines`, {
      method: 'POST', headers: headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ ...request, semanticHash: 'not-a-hash' }),
    })
    expect(invalid.status).toBe(400)
  })

  it('disconnects a subject-bound provider without exposing a 501 path', async () => {
    const response = await fetch(`${base}/control/v1/provider-connections/plaid/connection-1`, { method: 'DELETE', headers: headers() })
    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({ provider: 'plaid', status: 'disconnected', message: 'Provider connection was disconnected.' })
    const absent = await fetch(`${base}/control/v1/provider-connections/mercury/connection-1`, { method: 'DELETE', headers: headers() })
    expect(absent.status).toBe(404)
  })

  it('fails closed for every authenticated control mutation without constructing a control when providers are disabled', async () => {
    const disabledHandler = createFinanceSyncHttpHandler({
      controlToken,
      providersEnabled: false,
      health: () => ({ status: 200, body: { ok: true } }),
    })
    const disabledServer = createServer((request, response) => { void disabledHandler(request, response) })
    await new Promise<void>((resolve, reject) => { disabledServer.once('error', reject); disabledServer.listen(0, '127.0.0.1', resolve) })
    const address = disabledServer.address()
    if (!address || typeof address === 'string') throw new Error('disabled server did not bind')
    const disabledBase = `http://127.0.0.1:${address.port}`
    try {
      const requests = [
        ['/control/v1/provider-connections', { method: 'GET' }],
        ['/control/v1/provider-connections/plaid/accounts', { method: 'GET' }],
        ['/control/v1/provider-connections/plaid/admissions/connection-1/account-1', { method: 'GET' }],
        ['/control/v1/provider-connections/plaid/admit', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }],
        ['/control/v1/provider-connections/plaid/start', { method: 'POST' }],
        ['/control/v1/provider-connections/plaid/update', { method: 'POST' }],
        ['/control/v1/provider-connections/plaid/complete', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ publicToken: 'token', continuationToken: 'token' }) }],
        ['/control/v1/provider-connections/plaid/update/complete', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ continuationToken: 'token' }) }],
        ['/control/v1/provider-connections/plaid/connection-1', { method: 'DELETE' }],
        ['/control/v1/provider-connections/investec/start', { method: 'POST' }],
        ['/control/v1/provider-connections/mercury/start', { method: 'POST' }],
        ['/control/v1/provider-connections/snaptrade/start', { method: 'POST' }],
        ['/control/v1/observation-quarantines', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }],
        ['/control/v1/investec/card-currency-corrections', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }],
      ] as const
      for (const [path, init] of requests) {
        const response = await fetch(`${disabledBase}${path}`, { ...init, headers: headers('headers' in init ? init.headers : {}) })
        expect(response.status).toBe(503)
        expect(await response.json()).toEqual({ error: 'provider_control_disabled' })
      }
      const health = await fetch(`${disabledBase}/health`)
      expect(health.status).toBe(200)
      expect(await health.json()).toEqual({ ok: true })
    } finally {
      await new Promise<void>(resolve => disabledServer.close(() => resolve()))
    }
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
