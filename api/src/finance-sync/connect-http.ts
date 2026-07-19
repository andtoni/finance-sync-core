import { createHash, timingSafeEqual } from 'node:crypto'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { ConnectControlError, FinanceSyncConnectControl } from './connect-control'

const MAX_JSON_BODY_BYTES = 16 * 1024

function tokenMatches(provided: string, expected: string): boolean {
  const left = createHash('sha256').update(provided).digest()
  const right = createHash('sha256').update(expected).digest()
  return timingSafeEqual(left, right)
}

function send(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }).end(JSON.stringify(body))
}

async function jsonBody(request: IncomingMessage): Promise<Record<string, unknown>> {
  if (request.headers['content-type']?.split(';', 1)[0]?.trim().toLowerCase() !== 'application/json') {
    throw new ConnectControlError(415, 'JSON content type is required')
  }
  const declaredLength = Number(request.headers['content-length'])
  if (Number.isFinite(declaredLength) && declaredLength > MAX_JSON_BODY_BYTES) {
    throw new ConnectControlError(413, 'request body is too large')
  }
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    size += value.length
    if (size > MAX_JSON_BODY_BYTES) throw new ConnectControlError(413, 'request body is too large')
    chunks.push(value)
  }
  let parsed: unknown
  try { parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')) } catch { throw new ConnectControlError(400, 'invalid JSON') }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new ConnectControlError(400, 'JSON object is required')
  return parsed as Record<string, unknown>
}

export function createFinanceSyncHttpHandler(input: {
  readonly controlToken: string
  readonly connect: FinanceSyncConnectControl
  readonly health: () => { readonly status: number; readonly body: unknown }
}) {
  return async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const path = new URL(request.url ?? '/', 'http://finance-sync.internal').pathname
    if (path === '/health') {
      const health = input.health()
      return send(response, health.status, health.body)
    }
    if (!path.startsWith('/control/v1/')) return send(response, 404, { error: 'not_found' })

    const authorization = request.headers.authorization
    const prefix = 'Bearer '
    if (!authorization?.startsWith(prefix) || !tokenMatches(authorization.slice(prefix.length), input.controlToken)) {
      return send(response, 401, { error: 'unauthorized' })
    }
    const rawSubject = request.headers['x-authenticated-subject']
    if (typeof rawSubject !== 'string' || !rawSubject) return send(response, 400, { error: 'request_failed' })

    try {
      if (request.method === 'GET' && path === '/control/v1/provider-connections') {
        return send(response, 200, await input.connect.list(rawSubject))
      }
      const accounts = path.match(/^\/control\/v1\/provider-connections\/([^/]+)\/accounts$/)
      if (request.method === 'GET' && accounts) {
        return send(response, 200, await input.connect.pendingAccounts(rawSubject, decodeURIComponent(accounts[1])))
      }
      const admissionStatus = path.match(/^\/control\/v1\/provider-connections\/([^/]+)\/admissions\/([^/]+)\/([^/]+)$/)
      if (request.method === 'GET' && admissionStatus) {
        return send(response, 200, await input.connect.admissionStatus(rawSubject,
          decodeURIComponent(admissionStatus[1]), decodeURIComponent(admissionStatus[2]), decodeURIComponent(admissionStatus[3])))
      }
      const admit = path.match(/^\/control\/v1\/provider-connections\/([^/]+)\/admit$/)
      if (request.method === 'POST' && admit) {
        const body = await jsonBody(request)
        const fields = ['authenticatedSubject', 'connectionId', 'providerAccountId', 'wealthfolioAccountId', 'databaseStateDigest'] as const
        if (!fields.every(field => typeof body[field] === 'string') || typeof body.providerGeneration !== 'number'
          || typeof body.receiverAccessGeneration !== 'number'
          || (body.baselineKind !== 'empty' && body.baselineKind !== 'relink')
          || (body.priorProviderGeneration !== undefined && typeof body.priorProviderGeneration !== 'number')
          || !Array.isArray(body.assetMappings)) {
          throw new ConnectControlError(400, 'invalid admission request')
        }
        return send(response, 200, await input.connect.admit(rawSubject, decodeURIComponent(admit[1]), {
          authenticatedSubject: body.authenticatedSubject as string,
          connectionId: body.connectionId as string, providerAccountId: body.providerAccountId as string,
          wealthfolioAccountId: body.wealthfolioAccountId as string, databaseStateDigest: body.databaseStateDigest as string,
          providerGeneration: body.providerGeneration, receiverAccessGeneration: body.receiverAccessGeneration,
          baselineKind: body.baselineKind,
          ...(body.priorProviderGeneration === undefined ? {} : { priorProviderGeneration: body.priorProviderGeneration }),
          assetMappings: body.assetMappings as { providerAssetId: string; wealthfolioAssetId: string }[],
        }))
      }
      const start = path.match(/^\/control\/v1\/provider-connections\/([^/]+)\/start$/)
      if (request.method === 'POST' && start) {
        return send(response, 200, await input.connect.start(rawSubject, decodeURIComponent(start[1])))
      }
      if (request.method === 'POST' && path === '/control/v1/provider-connections/plaid/complete') {
        const body = await jsonBody(request)
        if (typeof body.publicToken !== 'string' || !body.publicToken || typeof body.continuationToken !== 'string' || !body.continuationToken) {
          throw new ConnectControlError(400, 'completion tokens are required')
        }
        return send(response, 200, await input.connect.completePlaid(rawSubject, body.publicToken, body.continuationToken))
      }
      return send(response, 404, { error: 'not_found' })
    } catch (error) {
      const status = error instanceof ConnectControlError ? error.status : 500
      return send(response, status, { error: 'request_failed' })
    }
  }
}
