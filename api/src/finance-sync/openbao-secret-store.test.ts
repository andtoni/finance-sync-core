import { describe, expect, it, vi } from 'vitest'
import { createOpenBaoPlaidTokenWriter, createOpenBaoProviderSecretReader } from './openbao-secret-store'

function scopedEnv(scope: 'READER' | 'PLAID_WRITER') {
  return {
    [`FINANCE_SYNC_OPENBAO_${scope}_AGENT_URL`]: 'https://bao-agent.example.test',
    [`FINANCE_SYNC_OPENBAO_${scope}_ENVIRONMENT`]: 'sandbox',
    [`FINANCE_SYNC_OPENBAO_${scope}_ALLOWED_SLOTS`]: scope === 'READER' ? 'PLAID_CONNECT,INVESTEC_BUNDLE' : 'PLAID_SLOT_A,PLAID_SLOT_B',
  }
}

describe('OpenBao Agent provider slots', () => {
  it('requires a complete finite exact allowlist', () => {
    expect(createOpenBaoProviderSecretReader({}).available).toBe(false)
    expect(() => createOpenBaoProviderSecretReader({ FINANCE_SYNC_OPENBAO_READER_AGENT_URL: 'http://bao-agent.example.test' })).toThrow('incomplete')
    expect(() => createOpenBaoProviderSecretReader({ ...scopedEnv('READER'), FINANCE_SYNC_OPENBAO_READER_ALLOWED_SLOTS: 'ONE,ONE' })).toThrow('finite')
    expect(() => createOpenBaoProviderSecretReader({ ...scopedEnv('READER'), FINANCE_SYNC_OPENBAO_READER_ALLOWED_SLOTS: 'ONE, TWO' })).toThrow('finite')
    expect(() => createOpenBaoPlaidTokenWriter({ ...scopedEnv('PLAID_WRITER'), FINANCE_SYNC_OPENBAO_PLAID_WRITER_ALLOWED_SLOTS: 'PLAID_SLOT_A' })).toThrow('finite')
  })

  it('resolves only reader-allowlisted opaque slots through the Agent', async () => {
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({ data: { data: { clientId: 'id', clientSecret: 'secret' } } }), { status: 200 })) as unknown as typeof fetch
    const reader = createOpenBaoProviderSecretReader(scopedEnv('READER'), fetchImpl)
    await expect(reader.resolve('PLAID_CONNECT')).resolves.toEqual({ clientId: 'id', clientSecret: 'secret' })
    expect(fetchImpl).toHaveBeenCalledWith('https://bao-agent.example.test/v1/secret/data/projects/finance-sync/sandbox/finance-sync/provider-slots/PLAID_CONNECT', expect.any(Object))
    await expect(reader.resolve('PLAID_SLOT_A')).rejects.toThrow('not allowlisted')
  })

  it('writer accepts only its supplied destination and never derives one', async () => {
    const fetchImpl = vi.fn(async () => new Response('{}', { status: 200 })) as unknown as typeof fetch
    const writer = createOpenBaoPlaidTokenWriter(scopedEnv('PLAID_WRITER'), fetchImpl)
    await expect(writer.storePlaidAccessToken({ subject: 'wealthfolio#owner', itemId: 'item-1', secretReference: 'PLAID_SLOT_B', clientId: 'client', clientSecret: 'secret', accessToken: 'token' })).resolves.toBe('PLAID_SLOT_B')
    expect(fetchImpl).toHaveBeenCalledWith('https://bao-agent.example.test/v1/secret/data/projects/finance-sync/sandbox/finance-sync/provider-slots/PLAID_SLOT_B', expect.objectContaining({ method: 'POST' }))
    await expect(writer.storePlaidAccessToken({ subject: 'wealthfolio#owner', itemId: 'item-1', secretReference: 'PLAID_SLOT_C', clientId: 'client', clientSecret: 'secret', accessToken: 'token' })).rejects.toThrow('not allowlisted')
  })
})
