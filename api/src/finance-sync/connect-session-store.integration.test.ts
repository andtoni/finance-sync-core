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

  it('rejects a slot already owned by another subject active connection', async () => {
    const deploymentId = 'cross-subject-slot-test'
    const principalId = '10000000-0000-4000-8000-000000000001'
    await sql`INSERT INTO provider_principals
      (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
      VALUES (${deploymentId}, 'wealthfolio#owner-a', 'plaid', ${principalId}::uuid, 'item-a')`
    await sql`INSERT INTO provider_connections
      (deployment_id, local_owner_id, provider_principal_id, provider, provider_connection_id, secret_reference, state)
      VALUES (${deploymentId}, 'wealthfolio#owner-a', ${principalId}::uuid, 'plaid', 'item-a', 'PLAID_SLOT_A', 'active')`
    const store = new PostgresConnectSessionStore(sql)
    await expect(store.create({ deploymentId, subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_A' })).resolves.toBeUndefined()
    const [count] = await sql`SELECT count(*)::int AS count FROM provider_connect_sessions WHERE deployment_id = ${deploymentId}`
    expect(count.count).toBe(0)
  })

  it('binds a continuation to deployment and subject and consumes it once', async () => {
    const store = new PostgresConnectSessionStore(sql)
    const token = await store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', secretReference: 'PLAID_SLOT_A' })
    expect(token).toBeDefined()
    expect(token!.length).toBeGreaterThanOrEqual(32)
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', token: token! })).resolves.toBeUndefined()
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token! })).resolves.toBe('PLAID_SLOT_A')
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token! })).resolves.toBeUndefined()
    // Consumption retains the lease during the write/onboarding window; release
    // makes an unreferenced slot immediately reusable after a failure.
    await expect(store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_A' })).resolves.toBeUndefined()
    await store.release({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token! })
    await expect(store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_A' })).resolves.toEqual(expect.any(String))
    const [row] = await sql`SELECT token_hash, selected_secret_reference, consumed_at IS NOT NULL consumed FROM provider_connect_sessions WHERE deployment_id='connect-session-test'`
    expect(row.consumed).toBe(true)
    expect(row.token_hash).not.toBe(token)
    expect(row.selected_secret_reference).toBe('PLAID_SLOT_A')
  })
})

afterAll(async () => { await sql?.end() })
