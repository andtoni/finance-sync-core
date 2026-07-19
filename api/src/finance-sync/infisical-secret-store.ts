import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import type { PlaidTokenWriter, ProviderSecretReader, ProviderSecretStore } from './connect-control'
import { unavailableProviderSecretStore } from './connect-control'

type Env = Readonly<Record<string, string | undefined>>

interface InfisicalConfig {
  readonly baseUrl: string
  readonly projectId: string
  readonly environment: string
  readonly secretPath: string
  readonly clientId: string
  readonly clientSecret: string
}

function configured(env: Env): InfisicalConfig | undefined {
  // Compose always supplies the in-container file path. Activation is controlled
  // only by the four non-secret identity fields so disabled boot remains valid.
  const activationNames = [
    'FINANCE_SYNC_INFISICAL_BASE_URL',
    'FINANCE_SYNC_INFISICAL_PROJECT_ID',
    'FINANCE_SYNC_INFISICAL_ENVIRONMENT',
    'FINANCE_SYNC_INFISICAL_CLIENT_ID',
  ] as const
  const present = activationNames.filter(name => Boolean(env[name]))
  if (present.length === 0) return undefined
  if (present.length !== activationNames.length || !env.FINANCE_SYNC_INFISICAL_CLIENT_SECRET_FILE) {
    throw new Error('Infisical ProviderSecretStore configuration is incomplete')
  }
  const url = new URL(env.FINANCE_SYNC_INFISICAL_BASE_URL!)
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('Infisical base URL must be credential-free HTTPS')
  }
  const clientSecret = readFileSync(env.FINANCE_SYNC_INFISICAL_CLIENT_SECRET_FILE, 'utf8').trim()
  if (!clientSecret) throw new Error('Infisical client secret file is empty')
  return {
    baseUrl: url.toString().replace(/\/$/, ''),
    projectId: env.FINANCE_SYNC_INFISICAL_PROJECT_ID!,
    environment: env.FINANCE_SYNC_INFISICAL_ENVIRONMENT!,
    secretPath: env.FINANCE_SYNC_INFISICAL_SECRET_PATH || '/finance-sync/provider-connections',
    clientId: env.FINANCE_SYNC_INFISICAL_CLIENT_ID!,
    clientSecret,
  }
}

async function jsonRequest(fetchImpl: typeof fetch, url: string, init: RequestInit): Promise<{ status: number; body: unknown }> {
  const response = await fetchImpl(url, init)
  let body: unknown
  try { body = await response.json() } catch { body = undefined }
  return { status: response.status, body }
}

function retryableStatus(status: number): boolean { return status === 429 || status >= 500 }
const pause = (attempt: number) => new Promise(resolve => setTimeout(resolve, 50 * (2 ** attempt)))

function scopedEnv(env: Env, scope: 'READER' | 'PLAID_WRITER'): Env {
  const names = ['BASE_URL', 'PROJECT_ID', 'ENVIRONMENT', 'SECRET_PATH', 'CLIENT_ID', 'CLIENT_SECRET_FILE'] as const
  return Object.fromEntries(names.map(name => [`FINANCE_SYNC_INFISICAL_${name}`, env[`FINANCE_SYNC_INFISICAL_${scope}_${name}`]]))
}

/** Legacy combined construction retained for compatibility; production uses split identities. */
export function createInfisicalProviderSecretStore(env: Env, fetchImpl: typeof fetch = fetch): ProviderSecretStore {
  const config = configured(env)
  if (!config) return unavailableProviderSecretStore
  let preparedToken: string | undefined

  const authenticate = async (): Promise<string> => {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      try {
        const login = await jsonRequest(fetchImpl, `${config.baseUrl}/api/v1/auth/universal-auth/login`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ clientId: config.clientId, clientSecret: config.clientSecret }),
        })
        const accessToken = typeof login.body === 'object' && login.body !== null
          ? (login.body as Record<string, unknown>).accessToken
          : undefined
        if (login.status === 200 && typeof accessToken === 'string' && accessToken) return accessToken
        if (!retryableStatus(login.status)) break
      } catch {
        // Transport failures are retryable; error details may contain secret material.
      }
      if (attempt < 3) await pause(attempt)
    }
    throw new Error('Infisical authentication failed')
  }

  const write = async (accessToken: string, method: 'PATCH' | 'POST', endpoint: string, body: string) =>
    jsonRequest(fetchImpl, endpoint, {
      method,
      headers: { authorization: `Bearer ${accessToken}`, 'content-type': 'application/json' },
      body,
    })

  const resolve = async (reference: string): Promise<Readonly<Record<string, unknown>>> => {
    if (!/^[A-Z0-9_]{1,256}$/.test(reference)) throw new Error('invalid provider secret reference')
    let accessToken = await authenticate()
    const endpoint = new URL(`${config.baseUrl}/api/v3/secrets/raw/${encodeURIComponent(reference)}`)
    endpoint.searchParams.set('workspaceId', config.projectId)
    endpoint.searchParams.set('environment', config.environment)
    endpoint.searchParams.set('secretPath', config.secretPath)
    for (let attempt = 0; attempt < 4; attempt += 1) {
      try {
        const result = await jsonRequest(fetchImpl, endpoint.toString(), { headers: { authorization: `Bearer ${accessToken}` } })
        const responseBody = typeof result.body === 'object' && result.body !== null
          ? result.body as Record<string, unknown> : undefined
        const secretBody = responseBody?.secret && typeof responseBody.secret === 'object' && !Array.isArray(responseBody.secret)
          ? responseBody.secret as Record<string, unknown> : undefined
        const value = secretBody?.secretValue ?? responseBody?.secretValue
        if (result.status >= 200 && result.status < 300 && typeof value === 'string' && value) {
          if (reference.startsWith('PLAID_ACCESS_TOKEN_')) return Object.freeze({ accessToken: value })
          let parsed: unknown
          try { parsed = JSON.parse(value) } catch { throw new Error('provider secret bundle is invalid') }
          if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('provider secret bundle is invalid')
          return Object.freeze(parsed as Record<string, unknown>)
        }
        if (result.status === 401 && attempt < 3) accessToken = await authenticate()
        else if (!retryableStatus(result.status)) throw new Error('Infisical secret read failed')
      } catch (error) {
        if (error instanceof Error && (error.message.startsWith('Infisical secret ') || error.message === 'provider secret bundle is invalid')) throw error
      }
      if (attempt < 3) await pause(attempt)
    }
    throw new Error('Infisical secret read failed')
  }

  return {
    available: true,
    resolve,
    async prepare(): Promise<void> {
      preparedToken = await authenticate()
    },
    async storePlaidAccessToken(input): Promise<string> {
      let accessToken = preparedToken
      preparedToken = undefined
      if (!accessToken) accessToken = await authenticate()
      const secretName = `PLAID_ACCESS_TOKEN_${createHash('sha256').update(`${input.subject}\0${input.itemId}`).digest('hex').toUpperCase()}`
      const endpoint = `${config.baseUrl}/api/v3/secrets/raw/${encodeURIComponent(secretName)}`
      const body = JSON.stringify({
        workspaceId: config.projectId,
        environment: config.environment,
        secretPath: config.secretPath,
        secretValue: input.accessToken,
        type: 'shared',
      })

      for (let attempt = 0; attempt < 4; attempt += 1) {
        try {
          let result = await write(accessToken, 'PATCH', endpoint, body)
          if (result.status >= 200 && result.status < 300) return secretName
          if (result.status === 401 && attempt < 3) {
            accessToken = await authenticate()
          } else if (result.status === 404) {
            result = await write(accessToken, 'POST', endpoint, body)
            if (result.status >= 200 && result.status < 300) return secretName
            if (result.status === 409) {
              result = await write(accessToken, 'PATCH', endpoint, body)
              if (result.status >= 200 && result.status < 300) return secretName
            }
            if (!retryableStatus(result.status)) throw new Error('Infisical secret creation failed')
          } else if (!retryableStatus(result.status)) {
            throw new Error('Infisical secret update failed')
          }
        } catch (error) {
          if (error instanceof Error && error.message.startsWith('Infisical secret ')) throw error
          // Retry transport failures without exposing response or request data.
        }
        if (attempt < 3) await pause(attempt)
      }
      throw new Error('Infisical secret persistence failed')
    },
  }
}

export function createInfisicalProviderSecretReader(env: Env, fetchImpl: typeof fetch = fetch): ProviderSecretReader {
  const combined = createInfisicalProviderSecretStore(scopedEnv(env, 'READER'), fetchImpl)
  return Object.freeze({ available: combined.available, resolve: combined.resolve.bind(combined) })
}

export function createInfisicalPlaidTokenWriter(env: Env, fetchImpl: typeof fetch = fetch): PlaidTokenWriter {
  const combined = createInfisicalProviderSecretStore(scopedEnv(env, 'PLAID_WRITER'), fetchImpl)
  return Object.freeze({
    available: combined.available,
    ...(combined.prepare ? { prepare: combined.prepare.bind(combined) } : {}),
    storePlaidAccessToken: combined.storePlaidAccessToken.bind(combined),
  })
}
