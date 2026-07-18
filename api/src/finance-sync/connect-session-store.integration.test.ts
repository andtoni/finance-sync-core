import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { PostgresConnectSessionStore } from './connect-session-store'
import { migrate } from './storage/migrate'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
let sql: ReturnType<typeof postgres>

suite('PostgreSQL provider Connect sessions', () => {
  beforeAll(async () => {
    sql = postgres(url!, { prepare: false })
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
  })

  it('binds a continuation to deployment and subject and consumes it once', async () => {
    const store = new PostgresConnectSessionStore(sql)
    const token = await store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid' })
    expect(token.length).toBeGreaterThanOrEqual(32)
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', token })).resolves.toBe(false)
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token })).resolves.toBe(true)
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token })).resolves.toBe(false)
    const [row] = await sql`SELECT token_hash, consumed_at IS NOT NULL consumed FROM provider_connect_sessions WHERE deployment_id='connect-session-test'`
    expect(row.consumed).toBe(true)
    expect(row.token_hash).not.toBe(token)
  })
})

afterAll(async () => { await sql?.end() })
