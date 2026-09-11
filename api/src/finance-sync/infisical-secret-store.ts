import { randomUUID } from 'node:crypto'
import type { PlaidTokenWriter, ProviderSecretReader } from './connect-control'
import { unavailableProviderSecretStore } from './connect-control'
import { readExclusiveInput } from './secret-input'

type Env = Readonly<Record<string, string | undefined>>
type Scope = 'READER' | 'PLAID_WRITER'

const attempts = 4
const SLOT = /^[A-Z][A-Z0-9_]{0,127}$/
const PREFIX = /^[A-Z][A-Z0-9_]{2,63}_$/
const dynamicReference = (prefix: string) => `${prefix}${randomUUID().replaceAll('-', '').toUpperCase()}`
const isDynamicReference = (config: InfisicalConfig, reference: string) => new RegExp(`^${config.secretPrefix}[0-9A-F]{32}$`).test(reference)
const reservationBinding = (value: string | undefined): string | undefined => /^[0-9a-f]{64}$/.test(value ?? '') ? value : undefined
const reservationPlaceholder = (binding: string) => ({ schema: 'finance-sync-plaid-reservation-v1', binding })
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

interface InfisicalConfig {
  readonly baseUrl: string
  readonly projectId: string
  readonly environment: string
  readonly secretPath: string
  readonly clientId: string
  readonly clientSecret: string
  readonly allowedSlots: readonly string[]
  readonly secretPrefix: string
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
  const names = ['URL', 'PROJECT_ID', 'ENVIRONMENT', 'SECRET_PATH', 'CLIENT_ID', 'CLIENT_SECRET', 'ALLOWED_SLOTS', 'SECRET_PREFIX'] as const
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

  const secretPrefix = input('SECRET_PREFIX')
  if (!PREFIX.test(secretPrefix)) throw new Error(`${prefix}SECRET_PREFIX is invalid`)
  return Object.freeze({
    baseUrl: url.toString().replace(/\/$/, ''), projectId, environment,
    secretPath: secretPath.length > 1 ? secretPath.replace(/\/$/, '') : '/',
    clientId: input('CLIENT_ID'), clientSecret: input('CLIENT_SECRET'),
    allowedSlots: allowedSlots(input('ALLOWED_SLOTS'), `${prefix}ALLOWED_SLOTS`, minimumSlots), secretPrefix,
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

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value && typeof value === 'object') return `{${Object.keys(value as Record<string, unknown>).sort().map(key => `${JSON.stringify(key)}:${canonical((value as Record<string, unknown>)[key])}`).join(',')}}`
  return JSON.stringify(value) ?? 'null'
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
  if ((!config.allowedSlots.includes(reference) && !new RegExp(`^${config.secretPrefix}[0-9A-F]{32}$`).test(reference)) || !SLOT.test(reference)) throw new Error('provider secret reference is not allowlisted')
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
    acceptsPlaidSecretReference: () => false,
    allocatePlaidSecretReference: undefined,
    resolve: unavailableProviderSecretStore.resolve.bind(unavailableProviderSecretStore),
    recoverPlaidAccessToken: unavailableProviderSecretStore.resolve.bind(unavailableProviderSecretStore),
    prepare: async () => { throw new Error('Infisical secret store is unavailable') },
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

  const prepare: PlaidTokenWriter['prepare'] = async reservation => {
    const token = await accessToken(config, fetchImpl)
    if (!reservation) return
    const reference = reservation.secretReference
    const url = secretUrl(config, reference)
    const dynamic = isDynamicReference(config, reference)
    if (!dynamic) throw new Error('Infisical Plaid destination is not a generated reservation')
    const binding = reservationBinding(reservation.reservationBinding)
    if (!binding) throw new Error('Infisical Plaid reservation binding is invalid')
    const placeholder = reservationPlaceholder(binding)
    const headers = { authorization: `Bearer ${token}`, 'content-type': 'application/json' }
    const result = await request(fetchImpl, url, {
      method: 'POST', headers,
      body: JSON.stringify({ projectId: config.projectId, environment: config.environment, secretPath: config.secretPath, secretValue: JSON.stringify(placeholder), secretComment: 'Finance Sync Plaid provider bundle reservation' }),
    })
    if (result.status === 200) return
    if (result.status === 400 || result.status === 409) {
      const existing = await request(fetchImpl, url, { headers })
      const existingBundle = secretBundle(existing.body)
      if (existing.status === 200 && existingBundle && canonical(existingBundle) === canonical(placeholder)) return
    }
    throw new Error('Infisical Plaid destination is not writable')
  }

  const recoverPlaidAccessToken: NonNullable<PlaidTokenWriter['recoverPlaidAccessToken']> = async reference => resolve(reference)

  const storePlaidAccessToken: PlaidTokenWriter['storePlaidAccessToken'] = async input => {
    const url = secretUrl(config, input.secretReference)
    const dynamic = isDynamicReference(config, input.secretReference)
    const binding = dynamic ? reservationBinding(input.reservationBinding) : undefined
    if (dynamic && !binding) throw new Error('Infisical Plaid reservation binding is required')
    const expected = { clientId: input.clientId, clientSecret: input.clientSecret, accessToken: input.accessToken, itemId: input.itemId }
    const secretValue = JSON.stringify(expected)
    const placeholder = binding ? reservationPlaceholder(binding) : undefined
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        const token = await accessToken(config, fetchImpl)
        const headers = { authorization: `Bearer ${token}`, 'content-type': 'application/json' }
        const existing = await request(fetchImpl, url, { headers })
        if (existing.status === 404) {
          if (!dynamic) throw new Error('Infisical provider slot is not provisioned')
          throw new Error('Infisical Plaid reservation is missing')
        }
        if (existing.status !== 200) {
          if (!retryable(existing.status)) break
          throw new Error('retry')
        }
        const existingBundle = secretBundle(existing.body)
        if (dynamic) {
          if (existingBundle && canonical(existingBundle) === canonical(expected)) return input.secretReference
          if (!existingBundle || !placeholder || canonical(existingBundle) !== canonical(placeholder)) throw new Error('Infisical secret persistence conflict')
        }
        const result = await request(fetchImpl, url, {
          method: 'PATCH', headers,
          body: JSON.stringify({ projectId: config.projectId, environment: config.environment, secretPath: config.secretPath, secretValue, secretComment: 'Finance Sync Plaid provider bundle' }),
        })
        if (result.status === 200) return input.secretReference
        if ((result.status === 400 || result.status === 409) && dynamic) {
          const retry = await request(fetchImpl, url, { headers })
          const retryBundle = secretBundle(retry.body)
          if (retry.status === 200 && retryBundle && canonical(retryBundle) === canonical(expected)) return input.secretReference
          throw new Error('Infisical secret persistence conflict')
        }
        if (!retryable(result.status)) break
      } catch (error) {
        if (error instanceof Error && (error.message === 'Infisical provider slot is not provisioned'
          || error.message === 'Infisical Plaid reservation is missing'
          || error.message === 'Infisical secret persistence conflict')) throw error
      }
      if (attempt < attempts - 1) await pause(attempt)
    }
    throw new Error('Infisical secret persistence failed')
  }

  const acceptsPlaidSecretReference = (reference: string) => SLOT.test(reference)
    && (config.allowedSlots.includes(reference) || isDynamicReference(config, reference))
  const allocatePlaidSecretReference = (_subject: string) => dynamicReference(config.secretPrefix)
  return { available: true, allowedSlots: config.allowedSlots, acceptsPlaidSecretReference, allocatePlaidSecretReference, prepare, recoverPlaidAccessToken, resolve, storePlaidAccessToken }
}

export function createInfisicalProviderSecretReader(env: Env, fetchImpl: typeof fetch = fetch): ProviderSecretReader {
  const store = createStore(env, 'READER', fetchImpl, 1)
  return Object.freeze({ available: store.available, resolve: store.resolve })
}

export function createInfisicalPlaidTokenWriter(env: Env, fetchImpl: typeof fetch = fetch): PlaidTokenWriter {
  const store = createStore(env, 'PLAID_WRITER', fetchImpl, 2)
  return Object.freeze({ available: store.available, allowedSlots: store.allowedSlots, acceptsPlaidSecretReference: store.acceptsPlaidSecretReference, allocatePlaidSecretReference: store.allocatePlaidSecretReference, prepare: store.prepare, recoverPlaidAccessToken: store.recoverPlaidAccessToken, storePlaidAccessToken: store.storePlaidAccessToken })
}
