import type { PlaidTokenWriter, ProviderSecretReader } from './connect-control'
import { unavailableProviderSecretStore } from './connect-control'
import { readExclusiveInput } from './secret-input'

type Env = Readonly<Record<string, string | undefined>>

// The OpenBao Agent proxies this logical KV v2 API prefix using its auto-auth
// token. Callers can address only explicitly provisioned opaque slots.
const PROJECT_PATH = '/v1/secret/data/projects/finance-sync'
const attempts = 4
type OpenBaoEnvironment = 'sandbox' | 'production'

interface OpenBaoAgentConfig {
  readonly proxyUrl: string
  readonly environment: OpenBaoEnvironment
  readonly allowedSlots: readonly string[]
}

function opaqueSlot(reference: string): string {
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(reference)) throw new Error('invalid provider secret reference')
  return reference
}

/** A finite, exact comma-separated set; no paths, globs, whitespace, or duplicates. */
function allowedSlots(value: string | undefined, name: string, minimum = 1): readonly string[] {
  if (!value) throw new Error(`${name} is required`)
  const slots = value.split(',')
  if (slots.length < minimum || slots.length > 128 || slots.some(slot => !/^[A-Za-z0-9_-]{1,128}$/.test(slot)) || new Set(slots).size !== slots.length) {
    throw new Error(`${name} must be a finite comma-separated set of exact slots`)
  }
  return Object.freeze(slots)
}

function configured(env: Env, minimumSlots = 1): OpenBaoAgentConfig | undefined {
  const activationNames = ['FINANCE_SYNC_OPENBAO_AGENT_URL', 'FINANCE_SYNC_OPENBAO_ENVIRONMENT', 'FINANCE_SYNC_OPENBAO_ALLOWED_SLOTS'] as const
  const present = activationNames.filter(name => env[name] !== undefined || env[`${name}_FILE`] !== undefined)
  if (present.length === 0) return undefined
  if (present.length !== activationNames.length) throw new Error('OpenBao Agent configuration is incomplete')
  let url: URL
  try { url = new URL(readExclusiveInput(env, 'FINANCE_SYNC_OPENBAO_AGENT_URL', true)!) }
  catch { throw new Error('OpenBao Agent URL must be credential-free HTTPS') }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
    throw new Error('OpenBao Agent URL must be credential-free HTTPS')
  }
  const environment = readExclusiveInput(env, 'FINANCE_SYNC_OPENBAO_ENVIRONMENT', true)
  if (environment !== 'sandbox' && environment !== 'production') throw new Error('FINANCE_SYNC_OPENBAO_ENVIRONMENT must be sandbox or production')
  return { proxyUrl: url.toString().replace(/\/$/, ''), environment, allowedSlots: allowedSlots(readExclusiveInput(env, 'FINANCE_SYNC_OPENBAO_ALLOWED_SLOTS', true), 'FINANCE_SYNC_OPENBAO_ALLOWED_SLOTS', minimumSlots) }
}

function scopedEnv(env: Env, scope: 'READER' | 'PLAID_WRITER'): Env {
  const names = ['AGENT_URL', 'ENVIRONMENT', 'ALLOWED_SLOTS'] as const
  return Object.fromEntries(names.flatMap(name => [
    [`FINANCE_SYNC_OPENBAO_${name}`, env[`FINANCE_SYNC_OPENBAO_${scope}_${name}`]],
    [`FINANCE_SYNC_OPENBAO_${name}_FILE`, env[`FINANCE_SYNC_OPENBAO_${scope}_${name}_FILE`]],
  ]))
}
function retryableStatus(status: number): boolean { return status === 429 || status >= 500 }
const pause = (attempt: number) => new Promise(resolve => setTimeout(resolve, 50 * (2 ** attempt)))
async function jsonRequest(fetchImpl: typeof fetch, url: string, init: RequestInit): Promise<{ status: number; body: unknown }> {
  const response = await fetchImpl(url, { ...init, redirect: 'error' })
  let body: unknown
  try { body = await response.json() } catch { body = undefined }
  return { status: response.status, body }
}
function endpoint(config: OpenBaoAgentConfig, reference: string): string {
  if (!config.allowedSlots.includes(opaqueSlot(reference))) throw new Error('provider secret reference is not allowlisted')
  return `${config.proxyUrl}${PROJECT_PATH}/${config.environment}/finance-sync/provider-slots/${encodeURIComponent(reference)}`
}
function secretData(body: unknown): Readonly<Record<string, unknown>> | undefined {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return undefined
  const outer = body as Record<string, unknown>
  if (!outer.data || typeof outer.data !== 'object' || Array.isArray(outer.data)) return undefined
  const data = outer.data as Record<string, unknown>
  if (!data.data || typeof data.data !== 'object' || Array.isArray(data.data)) return undefined
  return Object.freeze({ ...(data.data as Record<string, unknown>) })
}
function createOpenBaoStore(env: Env, fetchImpl: typeof fetch = fetch, minimumSlots = 1) {
  const config = configured(env, minimumSlots)
  if (!config) return { available: false, allowedSlots: [] as readonly string[], resolve: unavailableProviderSecretStore.resolve.bind(unavailableProviderSecretStore), storePlaidAccessToken: unavailableProviderSecretStore.storePlaidAccessToken.bind(unavailableProviderSecretStore) }
  const resolve: ProviderSecretReader['resolve'] = async reference => {
    const url = endpoint(config, reference)
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try { const result = await jsonRequest(fetchImpl, url, {}); const data = secretData(result.body); if (result.status >= 200 && result.status < 300 && data) return data; if (!retryableStatus(result.status)) break } catch { /* redact */ }
      if (attempt < attempts - 1) await pause(attempt)
    }
    throw new Error('OpenBao secret read failed')
  }
  const storePlaidAccessToken: PlaidTokenWriter['storePlaidAccessToken'] = async input => {
    const url = endpoint(config, input.secretReference)
    const body = JSON.stringify({ data: { clientId: input.clientId, clientSecret: input.clientSecret, accessToken: input.accessToken } })
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try { const result = await jsonRequest(fetchImpl, url, { method: 'POST', headers: { 'content-type': 'application/json' }, body }); if (result.status >= 200 && result.status < 300) return input.secretReference; if (!retryableStatus(result.status)) break } catch { /* redact */ }
      if (attempt < attempts - 1) await pause(attempt)
    }
    throw new Error('OpenBao secret persistence failed')
  }
  return { available: true, allowedSlots: config.allowedSlots, resolve, storePlaidAccessToken }
}
export function createOpenBaoProviderSecretReader(env: Env, fetchImpl: typeof fetch = fetch): ProviderSecretReader {
  const store = createOpenBaoStore(scopedEnv(env, 'READER'), fetchImpl)
  return Object.freeze({ available: store.available, resolve: store.resolve })
}
export function createOpenBaoPlaidTokenWriter(env: Env, fetchImpl: typeof fetch = fetch): PlaidTokenWriter {
  const store = createOpenBaoStore(scopedEnv(env, 'PLAID_WRITER'), fetchImpl, 2)
  return Object.freeze({ available: store.available, allowedSlots: store.allowedSlots, storePlaidAccessToken: store.storePlaidAccessToken })
}
