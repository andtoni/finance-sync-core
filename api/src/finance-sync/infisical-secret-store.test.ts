import { describe, expect, it, vi } from 'vitest'
import { createInfisicalPlaidTokenWriter, createInfisicalProviderSecretReader } from './infisical-secret-store'

const projectId = '00000000-0000-4000-8000-000000000001'
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
  }
}
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })

function loginThen(...responses: Response[]) {
  const fetchImpl = vi.fn<typeof fetch>()
  fetchImpl.mockResolvedValueOnce(json({ accessToken: 'short-lived-token' }))
  for (const response of responses) fetchImpl.mockResolvedValueOnce(response)
  return fetchImpl
}

describe('Infisical provider slots', () => {
  it('is unavailable by default and rejects partial or broad configuration', () => {
    expect(createInfisicalProviderSecretReader({}).available).toBe(false)
    expect(() => createInfisicalProviderSecretReader({ FINANCE_SYNC_INFISICAL_READER_URL: 'https://secrets.example.test' })).toThrow('incomplete')
    expect(() => createInfisicalProviderSecretReader(scopedEnv('READER', 'ONE,ONE'))).toThrow('finite')
    expect(() => createInfisicalProviderSecretReader(scopedEnv('READER', '*'))).toThrow('finite')
    expect(() => createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER', 'PLAID_SLOT_A'))).toThrow('finite')
  })

  it('authenticates with Universal Auth and reads one exact JSON bundle', async () => {
    const fetchImpl = loginThen(json({ secret: { secretValue: JSON.stringify({ clientId: 'id', clientSecret: 'secret' }) } }))
    const reader = createInfisicalProviderSecretReader(scopedEnv('READER'), fetchImpl)

    await expect(reader.resolve('PLAID_CONNECT')).resolves.toEqual({ clientId: 'id', clientSecret: 'secret' })
    expect(fetchImpl).toHaveBeenNthCalledWith(1, 'https://secrets.example.test/api/v1/auth/universal-auth/login', expect.objectContaining({ method: 'POST', redirect: 'error' }))
    expect(fetchImpl.mock.calls[1]![0].toString()).toContain('/api/v4/secrets/PLAID_CONNECT?')
    expect(fetchImpl.mock.calls[1]![0].toString()).toContain('secretPath=%2Ffinance-sync%2Fprovider-slots')
    expect(fetchImpl.mock.calls[1]![1]).toEqual(expect.objectContaining({ redirect: 'error', headers: { authorization: 'Bearer short-lived-token' } }))
  })

  it('fails closed for a reference outside the exact slot allowlist', async () => {
    const reader = createInfisicalProviderSecretReader(scopedEnv('READER'), vi.fn<typeof fetch>())
    await expect(reader.resolve('MERCURY_BUNDLE')).rejects.toThrow('not allowlisted')
  })

  it('updates only a pre-provisioned Plaid slot and never returns the token', async () => {
    const fetchImpl = loginThen(json({ secret: { secretValue: '{}' } }), json({ secret: { secretValue: '{}' } }))
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), fetchImpl)
    await expect(writer.storePlaidAccessToken({
      subject: 'issuer:user', itemId: 'item-1', secretReference: 'PLAID_SLOT_A',
      clientId: 'client', clientSecret: 'secret', accessToken: 'access',
    })).resolves.toBe('PLAID_SLOT_A')

    expect(fetchImpl).toHaveBeenCalledTimes(3)
    const patch = fetchImpl.mock.calls[2]!
    expect(patch[1]).toEqual(expect.objectContaining({ method: 'PATCH', redirect: 'error' }))
    const body = JSON.parse(patch[1]!.body as string)
    expect(JSON.parse(body.secretValue)).toEqual({ clientId: 'client', clientSecret: 'secret', accessToken: 'access' })
  })

  it('does not create an absent runtime slot', async () => {
    const fetchImpl = loginThen(json({}, 404))
    const writer = createInfisicalPlaidTokenWriter(scopedEnv('PLAID_WRITER'), fetchImpl)
    await expect(writer.storePlaidAccessToken({
      subject: 'issuer:user', itemId: 'item-1', secretReference: 'PLAID_SLOT_A',
      clientId: 'client', clientSecret: 'secret', accessToken: 'access',
    })).rejects.toThrow('not provisioned')
    expect(fetchImpl).toHaveBeenCalledTimes(2)
  })
})
