import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { createRuntimeDependencies } from './runtime-deps'
import { migrate } from './storage/migrate'
import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import type { RuntimeConnection } from './runtime'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
const runId = `${process.pid}-${Date.now()}`
const deploymentId = `runtime-deps-${runId}`
const owner = 'urn:wealthfolio:test#owner'
const sourceId = 'b0000000-0000-0000-0000-000000000030'
const connection: RuntimeConnection = {
  provider: 'plaid', connectionId: `connection-${runId}`, deploymentId, accessToken: 'test', localOwnerId: owner,
  sourceResourceIncarnationId: sourceId, accountIds: ['provider-account'],
  wealthfolioAccountIds: { 'provider-account': 'wealthfolio-account' }, accessGeneration: 1,
}
const { accessToken: _accessToken, ...connectionMetadata } = connection
const runtimeEnv = {
  FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([connectionMetadata]),
  FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify({ [connection.connectionId]: { accessToken: connection.accessToken } }),
  FINANCE_SYNC_PLAID_URL: 'https://sandbox.plaid.com', FINANCE_SYNC_PLAID_CLIENT_ID: 'test', FINANCE_SYNC_PLAID_SECRET: 'test',
  FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
}
let sql: ReturnType<typeof postgres>

function observation(transactionId: string, payload: Record<string, unknown>): ObservationOutput {
  return {
    deploymentId, localOwnerId: owner, sourceResourceIncarnationId: sourceId,
    providerVersion: `plaid:txn:${transactionId}`, semanticHash: transactionId.padEnd(64, 'a').slice(0, 64),
    hashSpecificationVersion: 1, domain: 'cash_ledger', payload,
    observedAt: '2026-07-17T00:00:00.000Z',
  }
}

function binding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v1;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
}

suite('runtime dependencies with PostgreSQL', () => {
  beforeAll(async () => {
    sql = postgres(url!, { prepare: false })
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
    await sql`INSERT INTO provider_principals (deployment_id,local_owner_id,provider,provider_principal_id,provider_subject_id) VALUES (${deploymentId},${owner},'plaid','b0000000-0000-0000-0000-000000000001','subject')`
    await sql`INSERT INTO connection_incarnations (deployment_id,local_owner_id,connection_incarnation_id,provider_principal_id,provider_connection_id,incarnation) VALUES (${deploymentId},${owner},'b0000000-0000-0000-0000-000000000010','b0000000-0000-0000-0000-000000000001',${connection.connectionId},1)`
    await sql`INSERT INTO account_incarnations (deployment_id,local_owner_id,account_incarnation_id,connection_incarnation_id,provider_account_id,incarnation) VALUES (${deploymentId},${owner},'b0000000-0000-0000-0000-000000000020','b0000000-0000-0000-0000-000000000010','provider-account',1)`
    await sql`INSERT INTO resource_incarnations (deployment_id,local_owner_id,source_resource_incarnation_id,account_incarnation_id,resource_kind,provider_resource_id,incarnation) VALUES (${deploymentId},${owner},${sourceId},'b0000000-0000-0000-0000-000000000020','cash_ledger','resource',1)`
  })

  it('rolls observation and outbox back atomically, then recovers on replay', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    await sql.unsafe(`CREATE FUNCTION reject_runtime_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.deployment_id = '${deploymentId}' THEN RAISE EXCEPTION 'test crash'; END IF; RETURN NEW; END $$`)
    await sql.unsafe('CREATE TRIGGER reject_runtime_outbox BEFORE INSERT ON projection_outbox FOR EACH ROW EXECUTE FUNCTION reject_runtime_outbox()')
    const item = observation('atomic', { transactionId: 'atomic', accountId: 'provider-account', amount: '1', currency: 'USD', date: '2026-07-17' })
    await expect(deps.persistAndEnqueue(connection, [item])).rejects.toThrow(/test crash/)
    const [rolledBack] = await sql`SELECT (SELECT count(*)::int FROM raw_observations WHERE deployment_id=${deploymentId}) observations, (SELECT count(*)::int FROM projection_outbox WHERE deployment_id=${deploymentId}) outbox`
    expect(rolledBack).toEqual({ observations: 0, outbox: 0 })
    await sql`DROP TRIGGER reject_runtime_outbox ON projection_outbox`
    await sql`DROP FUNCTION reject_runtime_outbox()`
    await expect(deps.persistAndEnqueue(connection, [item])).resolves.toBe(1)
    const [recovered] = await sql`SELECT (SELECT count(*)::int FROM raw_observations WHERE deployment_id=${deploymentId}) observations, (SELECT count(*)::int FROM projection_outbox WHERE deployment_id=${deploymentId}) outbox`
    expect(recovered).toEqual({ observations: 1, outbox: 1 })
  })

  it('recovers removal account identity and enqueues a delete projection', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const original = observation('removed', { transactionId: 'removed', accountId: 'provider-account', amount: '2', currency: 'USD', date: '2026-07-17' })
    await deps.persistAndEnqueue(connection, [original])
    const removed = { ...observation('removed-tombstone', { transactionId: 'removed', isRemoved: true }), providerVersion: 'plaid:removed:removed' }
    await expect(deps.persistAndEnqueue(connection, [removed])).resolves.toBe(1)
    const [row] = await sql`SELECT payload FROM projection_outbox WHERE deployment_id=${deploymentId} AND payload->>'kind'='ActivityDelete'`
    expect(row.payload).toMatchObject({ accountId: 'wealthfolio-account', kind: 'ActivityDelete' })
  })

  it('concurrent workers claim each row once and a batch tail remains blocking', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    for (let index = 0; index < 30; index++) {
      const operation = { deploymentId, accountId: 'wealthfolio-account', idempotencyKey: `batch-${runId}-${index}`, subject: owner, accessGeneration: 1, kind: 'ActivityDelete', resourceId: `resource-${index}` }
      await sql`INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,idempotency_key,payload) VALUES (${deploymentId},${owner},${connection.connectionId},${operation.idempotencyKey},${sql.json(operation)})`
    }
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body)) as Record<string, unknown>
      const operation = 'activity_delete'
      const successor = null
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', operation, String(wire.resource_id), 'wealthfolio-account', '<null>', 'accepted']
      return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner, idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation, resourceId: wire.resource_id, lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 1, receiptBinding: binding(fields) }), { status: 200 })
    }) as typeof fetch
    try {
      const first = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      const second = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      const results = await Promise.all([first.projectPending(connection), second.projectPending(connection)])
      expect(results.reduce((sum, result) => sum + result.delivered, 0)).toBe(30)
      const [counts] = await sql`SELECT count(*) FILTER (WHERE state='delivered')::int delivered, max(attempts)::int max_attempts FROM projection_outbox WHERE deployment_id=${deploymentId}`
      expect(counts).toEqual({ delivered: 30, max_attempts: 1 })
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})

afterAll(async () => { await sql?.end() })
