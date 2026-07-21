import { createHash, randomBytes } from 'node:crypto'
import type { ConnectSessionStore } from './connect-control'
import type { FinanceSyncSql } from './storage/pg'

const SESSION_TTL_SECONDS = 10 * 60
function digest(token: string): string { return createHash('sha256').update(token, 'utf8').digest('hex') }

export class PostgresConnectSessionStore implements ConnectSessionStore {
  constructor(private readonly sql: FinanceSyncSql) {}
  async create(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly secretReference: string }): Promise<string | undefined> {
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(input.secretReference)) throw new Error('invalid selected secret reference')
    const token = randomBytes(32).toString('base64url')
    try {
      // Serialize allocation with Plaid connection persistence for this exact
      // deployment/slot and reject slots already owned by any subject.
      const reserved = await this.sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        await tx`SELECT pg_advisory_xact_lock(hashtext(${input.deploymentId}), hashtext(${input.secretReference}))`
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
        // The partial unique index is the live-session reservation mutex; remove
        // only expired leases so abandoned Link bootstrap can rotate later.
        await tx`DELETE FROM provider_connect_sessions WHERE deployment_id = ${input.deploymentId} AND provider = ${input.provider} AND selected_secret_reference = ${input.secretReference} AND released_at IS NULL AND expires_at <= now()`
        await tx`INSERT INTO provider_connect_sessions (deployment_id, subject, provider, token_hash, selected_secret_reference, expires_at) VALUES (${input.deploymentId}, ${input.subject}, ${input.provider}, ${digest(token)}, ${input.secretReference}, now() + (${SESSION_TTL_SECONDS} * interval '1 second'))`
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
  async consume(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string }): Promise<string | undefined> {
    if (input.token.length < 32 || input.token.length > 256) return undefined
    const rows = await this.sql<{ selected_secret_reference: string }[]>`
      UPDATE provider_connect_sessions SET consumed_at = now()
      WHERE deployment_id = ${input.deploymentId} AND subject = ${input.subject} AND provider = ${input.provider}
        AND token_hash = ${digest(input.token)} AND consumed_at IS NULL AND expires_at > now()
      RETURNING selected_secret_reference
    `
    return rows[0]?.selected_secret_reference
  }
  async release(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string }): Promise<void> {
    if (input.token.length < 32 || input.token.length > 256) return
    await this.sql`UPDATE provider_connect_sessions SET released_at = now()
      WHERE deployment_id = ${input.deploymentId} AND subject = ${input.subject} AND provider = ${input.provider}
        AND token_hash = ${digest(input.token)} AND released_at IS NULL`
  }
}
