import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { createInfisicalProviderSecretStore } from './infisical-secret-store'

function env() {
  const root = mkdtempSync(join(tmpdir(), 'finance-sync-infisical-'))
  const secretFile = join(root, 'client-secret')
  writeFileSync(secretFile, 'machine-secret\n', { mode: 0o600 })
  return {
    FINANCE_SYNC_INFISICAL_BASE_URL: 'https://secrets.example.test',
    FINANCE_SYNC_INFISICAL_PROJECT_ID: 'project-id',
    FINANCE_SYNC_INFISICAL_ENVIRONMENT: 'prod',
    FINANCE_SYNC_INFISICAL_SECRET_PATH: '/finance-sync/provider-connections',
    FINANCE_SYNC_INFISICAL_CLIENT_ID: 'client-id',
    FINANCE_SYNC_INFISICAL_CLIENT_SECRET_FILE: secretFile,
  }
}

describe('Infisical ProviderSecretStore', () => {
  it('is unavailable when intentionally unconfigured and rejects partial configuration', () => {
    expect(createInfisicalProviderSecretStore({}).available).toBe(false)
    expect(createInfisicalProviderSecretStore({
      FINANCE_SYNC_INFISICAL_CLIENT_SECRET_FILE: '/run/secrets/finance_sync_infisical_client_secret',
      FINANCE_SYNC_INFISICAL_SECRET_PATH: '/finance-sync/provider-connections',
    }).available).toBe(false)
    expect(() => createInfisicalProviderSecretStore({ FINANCE_SYNC_INFISICAL_BASE_URL: 'https://secrets.example.test' })).toThrow('incomplete')
  })

  it('authenticates one-shot and updates a deterministic secret without returning or logging values', async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = []
    const fetchImpl = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init })
      if (String(url).endsWith('/login')) return new Response(JSON.stringify({ accessToken: 'short-lived-token' }), { status: 200 })
      return new Response(JSON.stringify({ secret: {} }), { status: 200 })
    }) as unknown as typeof fetch
    const store = createInfisicalProviderSecretStore(env(), fetchImpl)
    expect(store.available).toBe(true)
    await store.prepare?.()
    await store.storePlaidAccessToken({ subject: 'wealthfolio#owner', itemId: 'item-1', accessToken: 'plaid-access-secret' })
    expect(calls).toHaveLength(2)
    expect(calls[1].init?.method).toBe('PATCH')
    expect(calls[1].url).toMatch(/\/api\/v4\/secrets\/PLAID_ACCESS_TOKEN_[A-F0-9]{64}$/)
    expect(JSON.parse(String(calls[1].init?.body))).toMatchObject({ projectId: 'project-id', environment: 'prod', secretValue: 'plaid-access-secret' })
  })

  it('retries transient writes using the prepared token before succeeding', async () => {
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ accessToken: 'short-lived-token' }), { status: 200 }))
      .mockResolvedValueOnce(new Response('{}', { status: 503 }))
      .mockResolvedValueOnce(new Response('{}', { status: 200 })) as unknown as typeof fetch
    const store = createInfisicalProviderSecretStore(env(), fetchImpl)
    await store.prepare?.()
    await store.storePlaidAccessToken({ subject: 'wealthfolio#owner', itemId: 'item-1', accessToken: 'secret' })
    expect(fetchImpl).toHaveBeenCalledTimes(3)
  })

  it('creates after a missing update and never retries non-404 failures', async () => {
    const fetchImpl = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ accessToken: 'short-lived-token' }), { status: 200 }))
      .mockResolvedValueOnce(new Response('{}', { status: 404 }))
      .mockResolvedValueOnce(new Response('{}', { status: 201 })) as unknown as typeof fetch
    await createInfisicalProviderSecretStore(env(), fetchImpl).storePlaidAccessToken({ subject: 'wealthfolio#owner', itemId: 'item-1', accessToken: 'secret' })
    expect(fetchImpl).toHaveBeenCalledTimes(3)
    expect((fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[2][1]?.method).toBe('POST')
  })
})
