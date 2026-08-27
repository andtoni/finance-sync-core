import { createHash, randomBytes } from 'node:crypto'
import type { ConsumedPlaidConnectSession, ConnectSessionStore, PlaidConnectFlow } from './connect-control'
import type { FinanceSyncSql } from './storage/pg'

const SESSION_TTL_SECONDS = 10 * 60
// Provider completion calls are bounded by the Finance Sync HTTP client timeout;
// keep a generous recovery window for a process that crashed after consume().
const CONSUMED_SESSION_RECOVERY_SECONDS = 15 * 60
function digest(token: string): string { return createHash('sha256').update(token, 'utf8').digest('hex') }

export class PostgresConnectSessionStore implements ConnectSessionStore {
  constructor(private readonly sql: FinanceSyncSql) {}
  async create(input: {
    readonly deploymentId: string
    readonly subject: string
    readonly provider: 'plaid'
    readonly secretReference: string
    readonly flow: PlaidConnectFlow
    readonly providerConnectionId?: string
    readonly providerGeneration?: number
  }): Promise<string | undefined> {
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(input.secretReference)) throw new Error('invalid selected secret reference')
    const flow = input.flow
    const providerConnectionId = input.providerConnectionId
    const providerGeneration = input.providerGeneration
    if (flow !== 'new' && flow !== 'update') throw new Error('invalid Plaid connect flow')
    if (flow === 'new' && (providerConnectionId !== undefined || providerGeneration !== undefined)) throw new Error('new Plaid sessions cannot bind an existing connection')
    if (flow === 'update' && (!providerConnectionId || providerConnectionId.length > 512 || providerConnectionId.includes('\0')
      || providerGeneration === undefined || !Number.isSafeInteger(providerGeneration) || providerGeneration < 1)) throw new Error('update Plaid sessions require a provider connection generation')
    const token = randomBytes(32).toString('base64url')
    try {
      // Serialize allocation with provider connection persistence for this exact
      // deployment/slot. New flows require an unused slot; update flows bind an
      // already-active Item without rotating its credential or generation.
      const reserved = await this.sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        await tx`SELECT pg_advisory_xact_lock(hashtext(${input.deploymentId}), hashtext(${input.secretReference}))`
        if (flow === 'new') {
          const [active] = await tx<{ active: boolean }[]>`
            SELECT EXISTS (
              SELECT 1 FROM provider_connections
              WHERE deployment_id = ${input.deploymentId}
                AND provider = 'plaid'
                AND secret_reference = ${input.secretReference}
                AND disabled_at IS NULL
            ) AS active
          `
          if (active?.active) return false
        } else {
          const [active] = await tx<{ active: boolean }[]>`
            SELECT EXISTS (
              SELECT 1 FROM provider_connections
              WHERE deployment_id = ${input.deploymentId}
                AND local_owner_id = ${input.subject}
                AND provider = 'plaid'
                AND provider_connection_id = ${providerConnectionId!}
                AND provider_generation = ${providerGeneration!}
                AND secret_reference = ${input.secretReference}
                AND state = 'active'
                AND disabled_at IS NULL
            ) AS active
          `
          if (!active?.active) return false
        }
        // The partial unique index is the live-session reservation mutex. An
        // unconsumed expired Link session is safe to delete; a consumed session
        // is retained until its bounded completion-recovery window has elapsed.
        await tx`DELETE FROM provider_connect_sessions WHERE deployment_id = ${input.deploymentId} AND provider = ${input.provider} AND selected_secret_reference = ${input.secretReference} AND released_at IS NULL AND consumed_at IS NULL AND expires_at <= now()`
        await tx`UPDATE provider_connect_sessions
          SET released_at = now()
          WHERE deployment_id = ${input.deploymentId} AND provider = ${input.provider}
            AND selected_secret_reference = ${input.secretReference} AND released_at IS NULL
            AND consumed_at IS NOT NULL
            AND consumed_at <= now() - (${CONSUMED_SESSION_RECOVERY_SECONDS} * interval '1 second')`
        await tx`INSERT INTO provider_connect_sessions (deployment_id, subject, provider, token_hash, selected_secret_reference, flow_kind, provider_connection_id, provider_generation, expires_at) VALUES (${input.deploymentId}, ${input.subject}, ${input.provider}, ${digest(token)}, ${input.secretReference}, ${flow}, ${providerConnectionId ?? null}, ${providerGeneration ?? null}, now() + (${SESSION_TTL_SECONDS} * interval '1 second'))`
        return true
      })
      return reserved ? token : undefined
    } catch (error) {
      // postgres exposes SQLSTATE on its error object; only a live-slot unique
      // conflict is a normal allocation miss. All infrastructure failures stay loud.
      if ((error as { code?: string }).code === '23505') return undefined
      throw error
    }
  }
  async consume(input: {
    readonly deploymentId: string
    readonly subject: string
    readonly provider: 'plaid'
    readonly token: string
    readonly flow: PlaidConnectFlow
  }): Promise<ConsumedPlaidConnectSession | undefined> {
    if (input.token.length < 32 || input.token.length > 256) return undefined
    const flow = input.flow
    if (flow !== 'new' && flow !== 'update') return undefined
    const rows = await this.sql<{ selected_secret_reference: string; flow_kind: PlaidConnectFlow; provider_connection_id: string | null; provider_generation: number | null }[]>`
      UPDATE provider_connect_sessions SET consumed_at = now()
      WHERE deployment_id = ${input.deploymentId} AND subject = ${input.subject} AND provider = ${input.provider}
        AND token_hash = ${digest(input.token)} AND flow_kind = ${flow} AND consumed_at IS NULL AND released_at IS NULL AND expires_at > now()
      RETURNING selected_secret_reference, flow_kind, provider_connection_id, provider_generation
    `
    const row = rows[0]
    if (!row) return undefined
    return {
      secretReference: row.selected_secret_reference,
      flow: row.flow_kind,
      ...(row.provider_connection_id === null ? {} : { providerConnectionId: row.provider_connection_id }),
      ...(row.provider_generation === null ? {} : { providerGeneration: row.provider_generation }),
    }
  }
  async release(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string }): Promise<void> {
    if (input.token.length < 32 || input.token.length > 256) return
    await this.sql`UPDATE provider_connect_sessions SET released_at = now()
      WHERE deployment_id = ${input.deploymentId} AND subject = ${input.subject} AND provider = ${input.provider}
        AND token_hash = ${digest(input.token)} AND released_at IS NULL`
  }
}
