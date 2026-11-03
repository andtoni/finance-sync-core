import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Secret } from '@infisical/sdk'
import { createInfisicalPlaidTokenWriter, createInfisicalProviderSecretReader } from './infisical-secret-store'
import type { InfisicalClient, InfisicalClientFactory } from './infisical-secret-store'

const projectId = '00000000-0000-4000-8000-000000000001'
const location = { projectId, environment: 'prod', secretPath: '/finance-sync/provider-slots' }
function scopedEnv(scope: 'READER' | 'PLAID_WRITER', slots = scope === 'READER' ? 'PLAID_CONNECT,INVESTEC_BUNDLE' : 'PLAID_SLOT_A,PLAID_SLOT_B') {
  const prefix = `FINANCE_SYNC_INFISICAL_${scope}_`
  return {
    [`${prefix}URL`]: 'https://secrets.example.test',
    [`${prefix}PROJECT_ID`]: projectId,
    [`${prefix}ENVIRONMENT`]: 'prod',
    [`${prefix}SECRET_PATH`]: '/finance-sync/provider-slots',
    [`${prefix}CLIENT_ID`]: 'machine-client',
    [`${prefix}CLIENT_SECRET`]: 'machine-secret',
    [`${prefix}ALLOWED_SLOTS`]: slots,
    [`${prefix}SECRET_PREFIX`]: 'FINANCE_SYNC_PLAID_ITEM_',
  }
}

type Secrets = ReturnType<InfisicalClient['secrets']>
const stored = (secretValue: string) => ({ secretValue }) as Secret
const written = {} as never
/** The two error shapes @infisical/sdk 5.0.2 raises for an HTTP failure. */
const rejected = (status: number) => new Error(`[URL=https://secrets.example.test/api/v3/secrets/raw/X] [Method=GET] [StatusCode=${status}] rejected`)
const rejectedWithoutMessage = (status: number) => new Error(`Request failed with status ${status}`)

/** An injected SDK client. Every login succeeds; secret calls are scripted per test. */
function sdk(accessToken: string | null = 'short-lived-token') {
  const login = vi.fn<ReturnType<InfisicalClient['auth']>['universalAuth']['login']>()
  const getSecret = vi.fn<Secrets['getSecret']>()
  const createSecret = vi.fn<Secrets['createSecret']>()
  const updateSecret = vi.fn<Secrets['updateSecret']>()
  const client: InfisicalClient = {
    auth: () => ({ universalAuth: { login }, getAccessToken: () => accessToken }),
    secrets: () => ({ getSecret, createSecret, updateSecret }),
  }
  login.mockResolvedValue(client)
  const createClient = vi.fn<InfisicalClientFactory>(() => client)
  return { createClient, login, getSecret, createSecret, updateSecret }
}

const reference = 'FINANCE_SYNC_PLAID_ITEM_0123456789ABCDEF0123456789ABCDEF'
const binding = 'a'.repeat(64)
const placeholder = JSON.stringify({ schema: 'finance-sync-plaid-reservation-v1', binding })
const slotInput = { subject: 'issuer:user', itemId: 'item-1', secretReference: 'PLAID_SLOT_A', clientId: 'client', clientSecret: 'secret', accessToken: 'access' }
const generatedInput = { ...slotInput, secretReference: reference, reservationBinding: binding }
const bundle = { clientId: 'client', clientSecret: 'secret', accessToken: 'access', itemId: 'item-1' }

describe('Infisical provider slots', () => {
  it('allocates dynamic references without an artificial connection count', () => {
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'))
    const first = writer.allocatePlaidSecretReference!('wealthfolio#owner-a')
    const second = writer.allocatePlaidSecretReference!('wealthfolio#owner-a')
    expect(first).toMatch(/^FINANCE_SYNC_PLAID_ITEM_[0-9A-F]{32}$/)
    expect(second).toMatch(/^FINANCE_SYNC_PLAID_ITEM_[0-9A-F]{32}$/)
    expect(second).not.toBe(first)
    expect(writer.acceptsPlaidSecretReference!(first)).toBe(true)
    expect(writer.acceptsPlaidSecretReference!('FINANCE_SYNC_PLAID_ITEM_EVIL')).toBe(false)
  })
  it('is unavailable by default and rejects partial or broad configuration', () => {
    expect(createInfisicalProviderSecretReader({}).available).toBe(false)
    expect(() => createInfisicalProviderSecretReader({ FINANCE_SYNC_INFISICAL_READER_URL: 'https://secrets.example.test' })).toThrow('incomplete')
    expect(() => createInfisicalProviderSecretReader(scopedEnv('READER', 'ONE,ONE'))).toThrow('finite')
    expect(() => createInfisicalProviderSecretReader(scopedEnv('READER', '*'))).toThrow('finite')
    expect(() => createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER', 'PLAID_SLOT_A'))).toThrow('finite')
  })

  it('authenticates with Universal Auth and reads one exact JSON bundle', async () => {
    const infisical = sdk()
    infisical.getSecret.mockResolvedValueOnce(stored(JSON.stringify({ clientId: 'id', clientSecret: 'secret' })))
    const reader = createInfisicalProviderSecretReader(scopedEnv('READER'), infisical.createClient)

    await expect(reader.resolve('PLAID_CONNECT')).resolves.toEqual({ clientId: 'id', clientSecret: 'secret' })
    expect(infisical.createClient).toHaveBeenCalledExactlyOnceWith('https://secrets.example.test')
    expect(infisical.login).toHaveBeenCalledExactlyOnceWith({ clientId: 'machine-client', clientSecret: 'machine-secret' })
    expect(infisical.getSecret).toHaveBeenCalledExactlyOnceWith({ ...location, secretName: 'PLAID_CONNECT', expandSecretReferences: false, includeImports: false })
  })

  it('fails closed for a reference outside the exact slot allowlist', async () => {
    const infisical = sdk()
    const reader = createInfisicalProviderSecretReader(scopedEnv('READER'), infisical.createClient)
    await expect(reader.resolve('MERCURY_BUNDLE')).rejects.toThrow('not allowlisted')
    expect(infisical.createClient).not.toHaveBeenCalled()
  })

  it('updates only a pre-provisioned Plaid slot and never returns the token', async () => {
    const infisical = sdk()
    infisical.getSecret.mockResolvedValueOnce(stored('{}'))
    infisical.updateSecret.mockResolvedValueOnce(written)
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), infisical.createClient)
    await expect(writer.storePlaidAccessToken(slotInput)).resolves.toBe('PLAID_SLOT_A')

    expect(infisical.login).toHaveBeenCalledTimes(1)
    expect(infisical.getSecret).toHaveBeenCalledTimes(1)
    expect(infisical.createSecret).not.toHaveBeenCalled()
    expect(infisical.updateSecret).toHaveBeenCalledExactlyOnceWith('PLAID_SLOT_A', { ...location, secretValue: expect.any(String), secretComment: 'Finance Sync Plaid provider bundle' })
    expect(JSON.parse(infisical.updateSecret.mock.calls[0]![1].secretValue!)).toEqual(bundle)
  })

  it('treats the pinned HTTP 400 duplicate reservation create as idempotent', async () => {
    for (const status of [400, 409]) {
      const infisical = sdk()
      infisical.createSecret.mockRejectedValueOnce(rejected(status))
      infisical.getSecret.mockResolvedValueOnce(stored(placeholder))
      const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), infisical.createClient)
      await expect(writer.prepare!({ secretReference: reference, reservationBinding: binding })).resolves.toBeUndefined()
      expect(infisical.createSecret).toHaveBeenCalledExactlyOnceWith(reference, { ...location, secretValue: placeholder, secretComment: 'Finance Sync Plaid provider bundle reservation' })
      expect(infisical.getSecret).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ secretName: reference }))
    }

    const foreign = sdk()
    foreign.createSecret.mockRejectedValueOnce(rejected(400))
    foreign.getSecret.mockResolvedValueOnce(stored(JSON.stringify({ schema: 'finance-sync-plaid-reservation-v1', binding: 'b'.repeat(64) })))
    const foreignWriter = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), foreign.createClient)
    await expect(foreignWriter.prepare!({ secretReference: reference, reservationBinding: binding })).rejects.toThrow('Infisical Plaid destination is not writable')
  })

  it('proves generated create capability with a non-secret reservation before completion', async () => {
    const infisical = sdk()
    infisical.createSecret.mockResolvedValueOnce(written)
    infisical.getSecret.mockResolvedValueOnce(stored(placeholder))
    infisical.updateSecret.mockResolvedValueOnce(written)
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), infisical.createClient)
    await expect(writer.prepare!({ secretReference: reference, reservationBinding: binding })).resolves.toBeUndefined()
    expect(infisical.createSecret).toHaveBeenCalledTimes(1)
    expect(infisical.getSecret).not.toHaveBeenCalled()
    await expect(writer.storePlaidAccessToken(generatedInput)).resolves.toBe(reference)
    expect(infisical.login).toHaveBeenCalledTimes(2)
    expect(infisical.updateSecret).toHaveBeenCalledExactlyOnceWith(reference, expect.objectContaining({ secretValue: JSON.stringify(bundle) }))
  })

  it('rejects populated or foreign generated destinations without PATCH', async () => {
    const populated = sdk()
    populated.getSecret.mockResolvedValueOnce(stored(JSON.stringify({ ...bundle, clientId: 'other' })))
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), populated.createClient)
    await expect(writer.storePlaidAccessToken(generatedInput)).rejects.toThrow('Infisical secret persistence conflict')
    expect(populated.getSecret).toHaveBeenCalledTimes(1)
    expect(populated.updateSecret).not.toHaveBeenCalled()

    const foreign = sdk()
    foreign.getSecret.mockResolvedValueOnce(stored(JSON.stringify({ schema: 'finance-sync-plaid-reservation-v1', binding: 'b'.repeat(64) })))
    const foreignWriter = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), foreign.createClient)
    await expect(foreignWriter.storePlaidAccessToken(generatedInput)).rejects.toThrow('Infisical secret persistence conflict')
    expect(foreign.getSecret).toHaveBeenCalledTimes(1)
    expect(foreign.updateSecret).not.toHaveBeenCalled()
  })

  it('returns an identical populated generated bundle without PATCH', async () => {
    const infisical = sdk()
    infisical.getSecret.mockResolvedValueOnce(stored(JSON.stringify({ itemId: 'item-1', accessToken: 'access', clientSecret: 'secret', clientId: 'client' })))
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), infisical.createClient)
    await expect(writer.storePlaidAccessToken(generatedInput)).resolves.toBe(reference)
    expect(infisical.getSecret).toHaveBeenCalledTimes(1)
    expect(infisical.updateSecret).not.toHaveBeenCalled()
  })

  it('does not create an absent runtime slot', async () => {
    for (const notFound of [rejected(404), rejectedWithoutMessage(404)]) {
      const infisical = sdk()
      infisical.getSecret.mockRejectedValueOnce(notFound)
      const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), infisical.createClient)
      await expect(writer.storePlaidAccessToken(slotInput)).rejects.toThrow('Infisical provider slot is not provisioned')
      expect(infisical.getSecret).toHaveBeenCalledTimes(1)
      expect(infisical.createSecret).not.toHaveBeenCalled()
      expect(infisical.updateSecret).not.toHaveBeenCalled()

      const generated = sdk()
      generated.getSecret.mockRejectedValueOnce(notFound)
      const generatedWriter = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), generated.createClient)
      await expect(generatedWriter.storePlaidAccessToken(generatedInput)).rejects.toThrow('Infisical Plaid reservation is missing')
      expect(generated.createSecret).not.toHaveBeenCalled()
    }
  })

  it('retries 429, 5xx and transport failures with a fresh login, then redacts the failure', async () => {
    const infisical = sdk()
    infisical.getSecret
      .mockRejectedValueOnce(rejected(429))
      .mockRejectedValueOnce(rejectedWithoutMessage(503))
      .mockRejectedValueOnce(new Error('fetch failed'))
      .mockResolvedValueOnce(stored('{"clientId":"id"}'))
    const reader = createInfisicalProviderSecretReader(scopedEnv('READER'), infisical.createClient)
    await expect(reader.resolve('PLAID_CONNECT')).resolves.toEqual({ clientId: 'id' })
    expect(infisical.login).toHaveBeenCalledTimes(4)

    const down = sdk()
    down.getSecret.mockRejectedValue(rejected(500))
    const downReader = createInfisicalProviderSecretReader(scopedEnv('READER'), down.createClient)
    await expect(downReader.resolve('PLAID_CONNECT')).rejects.toThrow(/^Infisical secret read failed$/)
    expect(down.getSecret).toHaveBeenCalledTimes(4)

    const writer = sdk()
    writer.getSecret.mockResolvedValue(stored('{}'))
    writer.updateSecret.mockRejectedValueOnce(rejected(502)).mockResolvedValueOnce(written)
    const tokenWriter = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), writer.createClient)
    await expect(tokenWriter.storePlaidAccessToken(slotInput)).resolves.toBe('PLAID_SLOT_A')
    expect(writer.updateSecret).toHaveBeenCalledTimes(2)
  })

  it('does not retry a definitive rejection or a value that is not a JSON object', async () => {
    for (const script of [
      (get: ReturnType<typeof sdk>['getSecret']) => get.mockRejectedValue(rejected(403)),
      (get: ReturnType<typeof sdk>['getSecret']) => get.mockRejectedValue(rejected(404)),
      (get: ReturnType<typeof sdk>['getSecret']) => get.mockResolvedValue(stored('not-json')),
      (get: ReturnType<typeof sdk>['getSecret']) => get.mockResolvedValue(stored('[]')),
    ]) {
      const infisical = sdk()
      script(infisical.getSecret)
      const reader = createInfisicalProviderSecretReader(scopedEnv('READER'), infisical.createClient)
      await expect(reader.resolve('PLAID_CONNECT')).rejects.toThrow(/^Infisical secret read failed$/)
      expect(infisical.getSecret).toHaveBeenCalledTimes(1)
    }

    const forbidden = sdk()
    forbidden.getSecret.mockResolvedValue(stored('{}'))
    forbidden.updateSecret.mockRejectedValue(rejected(403))
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), forbidden.createClient)
    await expect(writer.storePlaidAccessToken(slotInput)).rejects.toThrow(/^Infisical secret persistence failed$/)
    expect(forbidden.updateSecret).toHaveBeenCalledTimes(1)
  })

  it('fails authentication without touching secrets when login is rejected or yields no token', async () => {
    const refused = sdk()
    refused.login.mockRejectedValue(rejected(401))
    const tokenless = sdk(null)
    for (const infisical of [refused, tokenless]) {
      const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), infisical.createClient)
      await expect(writer.prepare!()).rejects.toThrow(/^Infisical authentication failed$/)
      expect(infisical.createSecret).not.toHaveBeenCalled()
      expect(infisical.getSecret).not.toHaveBeenCalled()
    }
  })
})

describe('official Infisical SDK, with only the network replaced', () => {
  afterEach(() => vi.unstubAllGlobals())
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
  function network(...responses: Response[]) {
    const fetchImpl = vi.fn<typeof fetch>()
    fetchImpl.mockResolvedValueOnce(json({ accessToken: 'short-lived-token', expiresIn: 60 }))
    for (const response of responses) fetchImpl.mockResolvedValueOnce(response)
    vi.stubGlobal('fetch', fetchImpl)
    return fetchImpl
  }

  it('sends the scoped Universal Auth login and one exact, unexpanded secret read', async () => {
    const fetchImpl = network(json({ secret: { secretValue: JSON.stringify({ clientId: 'id', clientSecret: 'secret' }) } }))
    const reader = createInfisicalProviderSecretReader(scopedEnv('READER'))

    await expect(reader.resolve('PLAID_CONNECT')).resolves.toEqual({ clientId: 'id', clientSecret: 'secret' })
    expect(fetchImpl).toHaveBeenCalledTimes(2)
    expect(fetchImpl.mock.calls[0]![0]).toBe('https://secrets.example.test/api/v1/auth/universal-auth/login')
    expect(fetchImpl.mock.calls[0]![1]).toEqual(expect.objectContaining({ method: 'POST', body: JSON.stringify({ clientId: 'machine-client', clientSecret: 'machine-secret' }) }))
    const read = new URL(fetchImpl.mock.calls[1]![0] as string)
    expect(read.origin + read.pathname).toBe('https://secrets.example.test/api/v3/secrets/raw/PLAID_CONNECT')
    expect(Object.fromEntries(read.searchParams)).toEqual(expect.objectContaining({
      workspaceId: projectId, environment: 'prod', secretPath: '/finance-sync/provider-slots', expandSecretReferences: 'false', includeImports: 'false',
    }))
    expect(fetchImpl.mock.calls[1]![1]).toEqual(expect.objectContaining({ method: 'GET', headers: { Authorization: 'Bearer short-lived-token' } }))
  })

  it('recognises the status of real SDK errors, with and without a provider message', async () => {
    for (const notFound of [json({ message: 'Secret not found' }, 404), new Response(null, { status: 404 })]) {
      const fetchImpl = network(notFound)
      const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'))
      await expect(writer.storePlaidAccessToken(slotInput)).rejects.toThrow('Infisical provider slot is not provisioned')
      expect(fetchImpl).toHaveBeenCalledTimes(2)
    }

    const fetchImpl = network(json({ message: 'Secret already exist' }, 400), json({ secret: { secretValue: placeholder } }))
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'))
    await expect(writer.prepare!({ secretReference: reference, reservationBinding: binding })).resolves.toBeUndefined()
    expect(fetchImpl.mock.calls[1]![1]).toEqual(expect.objectContaining({ method: 'POST' }))
    expect(JSON.parse(fetchImpl.mock.calls[1]![1]!.body as string)).toEqual({
      workspaceId: projectId, environment: 'prod', secretPath: '/finance-sync/provider-slots', secretValue: placeholder, secretComment: 'Finance Sync Plaid provider bundle reservation',
    })
    expect(fetchImpl.mock.calls[2]![1]).toEqual(expect.objectContaining({ method: 'GET' }))
  })
})
