import type { PlaidTokenWriter, ProviderSecretReader } from './connect-control'
import { unavailableProviderSecretStore } from './connect-control'
import { readExclusiveInput } from './secret-input'

type Env = Readonly<Record<string, string | undefined>>
type Scope = 'READER' | 'PLAID_WRITER'

const attempts = 4
const SLOT = /^[A-Z][A-Z0-9_]{0,127}$/
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

interface InfisicalConfig {
  readonly baseUrl: string
  readonly projectId: string
  readonly environment: string
  readonly secretPath: string
  readonly clientId: string
  readonly clientSecret: string
  readonly allowedSlots: readonly string[]
}

function allowedSlots(value: string | undefined, name: string, minimum: number): readonly string[] {
  if (!value) throw new Error(`${name} is required`)
  const slots = value.split(',')
  if (slots.length < minimum || slots.length > 128 || slots.some(slot => !SLOT.test(slot)) || new Set(slots).size !== slots.length) {
    throw new Error(`${name} must be a finite comma-separated set of exact slots`)
  }
  return Object.freeze(slots)
}

function scoped(env: Env, scope: Scope, minimumSlots: number): InfisicalConfig | undefined {
  const prefix = `FINANCE_SYNC_INFISICAL_${scope}_`
  const names = ['URL', 'PROJECT_ID', 'ENVIRONMENT', 'SECRET_PATH', 'CLIENT_ID', 'CLIENT_SECRET', 'ALLOWED_SLOTS'] as const
  const present = names.filter(name => env[`${prefix}${name}`] !== undefined || env[`${prefix}${name}_FILE`] !== undefined)
  if (present.length === 0) return undefined
  if (present.length !== names.length) throw new Error('Infisical configuration is incomplete')

  const input = (name: typeof names[number]) => readExclusiveInput(env, `${prefix}${name}`, true)!
  let url: URL
  try { url = new URL(input('URL')) } catch { throw new Error('Infisical URL must be credential-free HTTPS') }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
    throw new Error('Infisical URL must be credential-free HTTPS')
  }
  const projectId = input('PROJECT_ID')
  if (!UUID.test(projectId)) throw new Error('Infisical project ID must be a UUID')
  const environment = input('ENVIRONMENT')
  if (!/^[a-z][a-z0-9_-]{0,63}$/.test(environment)) throw new Error('Infisical environment is invalid')
  const secretPath = input('SECRET_PATH')
  if (!/^\/(?:[A-Za-z0-9_-]+\/?)*$/.test(secretPath) || secretPath.includes('//')) throw new Error('Infisical secret path is invalid')

  return Object.freeze({
    baseUrl: url.toString().replace(/\/$/, ''),
    projectId,
    environment,
    secretPath: secretPath.length > 1 ? secretPath.replace(/\/$/, '') : '/',
    clientId: input('CLIENT_ID'),
    clientSecret: input('CLIENT_SECRET'),
    allowedSlots: allowedSlots(input('ALLOWED_SLOTS'), `${prefix}ALLOWED_SLOTS`, minimumSlots),
  })
}

const pause = (attempt: number) => new Promise(resolve => setTimeout(resolve, 50 * (2 ** attempt)))
const retryable = (status: number) => status === 429 || status >= 500

async function request(fetchImpl: typeof fetch, url: string, init: RequestInit): Promise<{ status: number; body: unknown }> {
  const response = await fetchImpl(url, { ...init, redirect: 'error' })
  let body: unknown
  try { body = await response.json() } catch { body = undefined }
  return { status: response.status, body }
}

function object(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined
}

async function accessToken(config: InfisicalConfig, fetchImpl: typeof fetch): Promise<string> {
  const result = await request(fetchImpl, `${config.baseUrl}/api/v1/auth/universal-auth/login`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ clientId: config.clientId, clientSecret: config.clientSecret }),
  })
  const token = object(result.body)?.accessToken
  if (result.status !== 200 || typeof token !== 'string' || !token) throw new Error('Infisical authentication failed')
  return token
}

function secretUrl(config: InfisicalConfig, reference: string): string {
  if (!config.allowedSlots.includes(reference) || !SLOT.test(reference)) throw new Error('provider secret reference is not allowlisted')
  const query = new URLSearchParams({
    projectId: config.projectId,
    environment: config.environment,
    secretPath: config.secretPath,
    expandSecretReferences: 'false',
    includeImports: 'false',
  })
  return `${config.baseUrl}/api/v4/secrets/${encodeURIComponent(reference)}?${query}`
}

function secretBundle(body: unknown): Readonly<Record<string, unknown>> | undefined {
  const value = object(object(body)?.secret)?.secretValue
  if (typeof value !== 'string') return undefined
  try {
    const parsed = object(JSON.parse(value))
    return parsed ? Object.freeze({ ...parsed }) : undefined
  } catch { return undefined }
}

function createStore(env: Env, scope: Scope, fetchImpl: typeof fetch, minimumSlots: number) {
  const config = scoped(env, scope, minimumSlots)
  if (!config) return {
    available: false,
    allowedSlots: [] as readonly string[],
    resolve: unavailableProviderSecretStore.resolve.bind(unavailableProviderSecretStore),
    storePlaidAccessToken: unavailableProviderSecretStore.storePlaidAccessToken.bind(unavailableProviderSecretStore),
  }

  const resolve: ProviderSecretReader['resolve'] = async reference => {
    const url = secretUrl(config, reference)
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        const token = await accessToken(config, fetchImpl)
        const result = await request(fetchImpl, url, { headers: { authorization: `Bearer ${token}` } })
        const bundle = secretBundle(result.body)
        if (result.status === 200 && bundle) return bundle
        if (!retryable(result.status)) break
      } catch { /* redact all transport and provider responses */ }
      if (attempt < attempts - 1) await pause(attempt)
    }
    throw new Error('Infisical secret read failed')
  }

  const storePlaidAccessToken: PlaidTokenWriter['storePlaidAccessToken'] = async input => {
    const url = secretUrl(config, input.secretReference)
    const secretValue = JSON.stringify({ clientId: input.clientId, clientSecret: input.clientSecret, accessToken: input.accessToken })
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        const token = await accessToken(config, fetchImpl)
        const headers = { authorization: `Bearer ${token}`, 'content-type': 'application/json' }
        const existing = await request(fetchImpl, url, { headers })
        if (existing.status === 404) throw new Error('Infisical provider slot is not provisioned')
        if (existing.status !== 200) {
          if (!retryable(existing.status)) break
          throw new Error('retry')
        }
        const result = await request(fetchImpl, url, {
          method: 'PATCH', headers,
          body: JSON.stringify({
            projectId: config.projectId,
            environment: config.environment,
            secretPath: config.secretPath,
            secretValue,
            secretComment: 'Finance Sync Plaid provider bundle',
          }),
        })
        if (result.status === 200) return input.secretReference
        if (!retryable(result.status)) break
      } catch (error) {
        if (error instanceof Error && error.message === 'Infisical provider slot is not provisioned') throw error
      }
      if (attempt < attempts - 1) await pause(attempt)
    }
    throw new Error('Infisical secret persistence failed')
  }

  return { available: true, allowedSlots: config.allowedSlots, resolve, storePlaidAccessToken }
}

export function createInfisicalProviderSecretReader(env: Env, fetchImpl: typeof fetch = fetch): ProviderSecretReader {
  const store = createStore(env, 'READER', fetchImpl, 1)
  return Object.freeze({ available: store.available, resolve: store.resolve })
}

export function createInfisicalPlaidTokenWriter(env: Env, fetchImpl: typeof fetch = fetch): PlaidTokenWriter {
  const store = createStore(env, 'PLAID_WRITER', fetchImpl, 2)
  return Object.freeze({ available: store.available, allowedSlots: store.allowedSlots, storePlaidAccessToken: store.storePlaidAccessToken })
}
