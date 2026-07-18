import { createHash, randomBytes } from 'node:crypto'
import type { ConnectSessionStore } from './connect-control'
import type { FinanceSyncSql } from './storage/pg'

const SESSION_TTL_SECONDS = 10 * 60

function digest(token: string): string {
  return createHash('sha256').update(token, 'utf8').digest('hex')
}

export class PostgresConnectSessionStore implements ConnectSessionStore {
  constructor(private readonly sql: FinanceSyncSql) {}

  async create(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid' }): Promise<string> {
    const token = randomBytes(32).toString('base64url')
    const tokenHash = digest(token)
    await this.sql`
      INSERT INTO provider_connect_sessions
        (deployment_id, subject, provider, token_hash, expires_at)
      VALUES
        (${input.deploymentId}, ${input.subject}, ${input.provider}, ${tokenHash},
         now() + (${SESSION_TTL_SECONDS} * interval '1 second'))
    `
    return token
  }

  async consume(input: { readonly deploymentId: string; readonly subject: string; readonly provider: 'plaid'; readonly token: string }): Promise<boolean> {
    if (input.token.length < 32 || input.token.length > 256) return false
    const rows = await this.sql`
      UPDATE provider_connect_sessions
      SET consumed_at = now()
      WHERE deployment_id = ${input.deploymentId}
        AND subject = ${input.subject}
        AND provider = ${input.provider}
        AND token_hash = ${digest(input.token)}
        AND consumed_at IS NULL
        AND expires_at > now()
      RETURNING id
    `
    return rows.length === 1
  }
}
