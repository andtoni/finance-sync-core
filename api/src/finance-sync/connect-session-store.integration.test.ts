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
    await expect(store.create({ deploymentId, subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_A', flow: 'new' })).resolves.toBeUndefined()
    const [count] = await sql`SELECT count(*)::int AS count FROM provider_connect_sessions WHERE deployment_id = ${deploymentId}`
    expect(count.count).toBe(0)
  })

  it('binds a continuation to deployment and subject and consumes it once', async () => {
    const store = new PostgresConnectSessionStore(sql)
    const token = await store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', secretReference: 'PLAID_SLOT_A', flow: 'new' })
    expect(token).toBeDefined()
    expect(token!.length).toBeGreaterThanOrEqual(32)
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', token: token!, flow: 'new' })).resolves.toBeUndefined()
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })).resolves.toEqual({ secretReference: 'PLAID_SLOT_A', flow: 'new', claimVersion: 1 })
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })).resolves.toBeUndefined()
    await sql`UPDATE provider_connect_sessions SET completion_lease_until = now() - interval '1 second' WHERE deployment_id = 'connect-session-test'`
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })).resolves.toEqual({ secretReference: 'PLAID_SLOT_A', flow: 'new', recovery: true, claimVersion: 2 })
    // Recovery consumption retains the lease during the write/onboarding window; release
    // makes an unreferenced slot immediately reusable after a failure.
    await expect(store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_A', flow: 'new' })).resolves.toBeUndefined()
    await store.release({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-a', provider: 'plaid', token: token! })
    const reusableToken = await store.create({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_A', flow: 'new' })
    expect(reusableToken).toEqual(expect.any(String))
    await store.release({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', token: reusableToken! })
    await expect(store.consume({ deploymentId: 'connect-session-test', subject: 'wealthfolio#owner-b', provider: 'plaid', token: reusableToken!, flow: 'new' })).resolves.toBeUndefined()
    const [row] = await sql`SELECT token_hash, selected_secret_reference, flow_kind, provider_connection_id, provider_generation, consumed_at IS NOT NULL consumed FROM provider_connect_sessions WHERE deployment_id='connect-session-test' ORDER BY created_at ASC LIMIT 1`
    expect(row.consumed).toBe(true)
    expect(row.token_hash).not.toBe(token)
    expect(row.selected_secret_reference).toBe('PLAID_SLOT_A')
    expect(row.flow_kind).toBe('new')
    expect(row.provider_connection_id).toBeNull()
    expect(row.provider_generation).toBeNull()

    const recoveryDeployment = 'connect-session-recovery-test'
    const consumedToken = await store.create({ deploymentId: recoveryDeployment, subject: 'wealthfolio#owner-a', provider: 'plaid', secretReference: 'PLAID_SLOT_B', flow: 'new' })
    await store.consume({ deploymentId: recoveryDeployment, subject: 'wealthfolio#owner-a', provider: 'plaid', token: consumedToken!, flow: 'new' })
    await sql`UPDATE provider_connect_sessions SET consumed_at = now() - interval '16 minutes' WHERE deployment_id = ${recoveryDeployment}`
    await expect(store.create({ deploymentId: recoveryDeployment, subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_B', flow: 'new' })).resolves.toBeUndefined()
    await sql`UPDATE provider_connect_sessions SET completion_lease_until = now() - interval '1 second' WHERE deployment_id = ${recoveryDeployment}`
    const recoveredToken = await store.create({ deploymentId: recoveryDeployment, subject: 'wealthfolio#owner-b', provider: 'plaid', secretReference: 'PLAID_SLOT_B', flow: 'new' })
    expect(recoveredToken).toEqual(expect.any(String))
    const [recovered] = await sql`SELECT released_at IS NOT NULL AS released FROM provider_connect_sessions WHERE deployment_id = ${recoveryDeployment} ORDER BY created_at ASC LIMIT 1`
    expect(recovered.released).toBe(true)
  })

  it('atomically fences concurrent finalization and rejects a stale claim', async () => {
    const deploymentId = 'connect-finalize-fence-test'
    const store = new PostgresConnectSessionStore(sql)
    await sql`CREATE TEMP TABLE IF NOT EXISTS connect_finalize_probe (id integer)`
    const token = await store.create({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', secretReference: 'PLAID_SLOT_C', flow: 'new' })
    const claim = await store.consume({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })
    expect(claim?.claimVersion).toBe(1)
    const operation = async (tx: unknown) => { await (tx as typeof sql)`INSERT INTO connect_finalize_probe VALUES (1)` }
    const results = await Promise.allSettled([
      store.finalize({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, claimVersion: claim!.claimVersion!, itemId: 'item-finalize' }, operation),
      store.finalize({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, claimVersion: claim!.claimVersion!, itemId: 'item-finalize' }, operation),
    ])
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1)
    const [state] = await sql`SELECT completion_state FROM provider_connect_sessions WHERE deployment_id = ${deploymentId}`
    expect(state.completion_state).toBe('completed')
  })

  it('cannot release or finalize a newer recovery claim from a stale worker', async () => {
    const deploymentId = 'connect-stale-claim-test'
    const store = new PostgresConnectSessionStore(sql)
    const token = await store.create({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', secretReference: 'PLAID_SLOT_D', flow: 'new' })
    const first = await store.consume({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })
    await sql`UPDATE provider_connect_sessions SET completion_lease_until = now() - interval '1 second' WHERE deployment_id = ${deploymentId}`
    const second = await store.consume({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })
    expect(second?.claimVersion).toBe((first?.claimVersion ?? 0) + 1)
    await store.release({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, claimVersion: first!.claimVersion })
    const [live] = await sql`SELECT released_at, completion_claim_version FROM provider_connect_sessions WHERE deployment_id = ${deploymentId}`
    expect(live.released_at).toBeNull()
    await expect(store.finalize({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, claimVersion: first!.claimVersion!, itemId: 'stale-item' }, async () => undefined)).rejects.toThrow('stale')
    await expect(store.finalize({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, claimVersion: second!.claimVersion!, itemId: 'recovered-item' }, async () => undefined)).resolves.toBeUndefined()
    await expect(store.replayCompleted({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })).resolves.toEqual({ itemId: 'recovered-item' })
  })

  it('binds update continuations to the active Plaid Item without rotating its slot', async () => {
    const deploymentId = 'connect-update-session-test'
    const providerPrincipalId = '20000000-0000-4000-8000-000000000001'
    await sql`INSERT INTO provider_principals
      (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
      VALUES (${deploymentId}, 'wealthfolio#owner-a', 'plaid', ${providerPrincipalId}::uuid, 'item-update')`
    await sql`INSERT INTO provider_connections
      (deployment_id, local_owner_id, provider_principal_id, provider, provider_connection_id, secret_reference, state)
      VALUES (${deploymentId}, 'wealthfolio#owner-a', ${providerPrincipalId}::uuid, 'plaid', 'item-update', 'PLAID_SLOT_A', 'active')`
    const store = new PostgresConnectSessionStore(sql)
    const token = await store.create({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', secretReference: 'PLAID_SLOT_A', flow: 'update', providerConnectionId: 'item-update', providerGeneration: 1 })
    expect(token).toEqual(expect.any(String))
    await expect(store.consume({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'new' })).resolves.toBeUndefined()
    await expect(store.consume({ deploymentId, subject: 'wealthfolio#owner-a', provider: 'plaid', token: token!, flow: 'update' })).resolves.toEqual({ secretReference: 'PLAID_SLOT_A', flow: 'update', providerConnectionId: 'item-update', providerGeneration: 1, claimVersion: 1 })
  })
})

afterAll(async () => { await sql?.end() })
