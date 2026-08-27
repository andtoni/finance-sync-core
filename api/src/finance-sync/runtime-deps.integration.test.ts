import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { createRuntimeDependencies } from './runtime-deps'
import { migrate } from './storage/migrate'
import { currentStateToBridge } from './projection/bridge-client'
import { currentStateSemanticHash } from './projection/current-state'
import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import type { RuntimeConnection } from './runtime'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
const runId = `${process.pid}-${Date.now()}`
const deploymentId = `runtime-deps-${runId}`
const owner = 'urn:wealthfolio:test#owner'
const sourceId = 'b0000000-0000-0000-0000-000000000030'
const connection: RuntimeConnection = {
  provider: 'plaid', connectionId: `connection-${runId}`, deploymentId, clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test', localOwnerId: owner,
  sourceResourceIncarnationId: sourceId, accountIds: ['provider-account'],
  wealthfolioAccountIds: { 'provider-account': 'wealthfolio-account' }, providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT',
}
const { accessToken: _accessToken, clientId: _clientId, clientSecret: _clientSecret, ...connectionMetadata } = connection
const runtimeEnv = {
  FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([connectionMetadata]),
  FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify({ [connection.connectionId]: {
    accessToken: connection.accessToken, clientId: connection.clientId, clientSecret: connection.clientSecret,
  } }),
  FINANCE_SYNC_PLAID_URL: 'https://sandbox.plaid.com',
  FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts', FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
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

function balanceObservation(): ObservationOutput {
  return {
    deploymentId, localOwnerId: owner, sourceResourceIncarnationId: sourceId,
    providerVersion: 'plaid:balance:provider-account', semanticHash: 'b'.repeat(64),
    hashSpecificationVersion: 1, domain: 'cash_ledger',
    payload: { accountId: 'provider-account', currentBalance: '100', currency: 'USD', asOfDate: '2026-07-17' },
    observedAt: '2026-07-17T00:00:00.000Z',
  }
}

function binding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v3;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
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
    const items = [
      observation('atomic', { transactionId: 'atomic', accountId: 'provider-account', amount: '1', currency: 'USD', date: '2026-07-17' }),
      balanceObservation(),
    ]
    try {
      await expect(deps.persistAndEnqueue(connection, items)).rejects.toThrow(/test crash/)
      const [rolledBack] = await sql`SELECT (SELECT count(*)::int FROM raw_observations WHERE deployment_id=${deploymentId}) observations, (SELECT count(*)::int FROM projection_outbox WHERE deployment_id=${deploymentId}) outbox`
      expect(rolledBack).toEqual({ observations: 0, outbox: 0 })
    } finally {
      await sql`DROP TRIGGER IF EXISTS reject_runtime_outbox ON projection_outbox`
      await sql`DROP FUNCTION IF EXISTS reject_runtime_outbox()`
    }
    await expect(deps.persistAndEnqueue(connection, items)).resolves.toBe(2)
    const [recovered] = await sql`SELECT (SELECT count(*)::int FROM raw_observations WHERE deployment_id=${deploymentId}) observations, (SELECT count(*)::int FROM projection_outbox WHERE deployment_id=${deploymentId}) outbox`
    expect(recovered).toEqual({ observations: 2, outbox: 2 })
  })

  it('rolls every grouped batch back when a later batch member fails authority', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const transactionId = `grouped-${runId}`
    const valid = observation(transactionId, {
      transactionId, accountId: 'provider-account', amount: '3', currency: 'USD', date: '2026-07-17',
    })
    const invalid = { ...valid, sourceResourceIncarnationId: 'b0000000-0000-0000-0000-000000000099' }
    await expect(deps.persistAndEnqueueBatch?.([
      { connection, observations: [valid], options: { skipCurrentState: true } },
      { connection, observations: [invalid], options: { skipCurrentState: true } },
    ])).rejects.toThrow(/observation authority mismatch/)
    const [counts] = await sql`SELECT
      (SELECT count(*)::int FROM raw_observations WHERE deployment_id=${deploymentId} AND payload->>'transactionId'=${transactionId}) observations,
      (SELECT count(*)::int FROM projection_outbox WHERE deployment_id=${deploymentId} AND payload->>'resourceId' LIKE ${`%${transactionId}`}) outbox`
    expect(counts).toEqual({ observations: 0, outbox: 0 })
  })

  it('recovers removal account identity but suppresses unresolved visible deletion', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const original = observation('removed', { transactionId: 'removed', accountId: 'provider-account', amount: '2', currency: 'USD', date: '2026-07-17' })
    await deps.persistAndEnqueue(connection, [original, balanceObservation()])
    const removed = { ...observation('removed-tombstone', { transactionId: 'removed', isRemoved: true }), providerVersion: 'plaid:removed:removed' }
    await expect(deps.persistAndEnqueue(connection, [removed, balanceObservation()])).resolves.toBe(1)
    const [counts] = await sql`SELECT
      count(*) FILTER (WHERE payload->>'kind'='ActivityDelete')::int deletes,
      (SELECT count(*)::int FROM raw_observations WHERE deployment_id=${deploymentId} AND payload->>'isRemoved'='true') removals
      FROM projection_outbox WHERE deployment_id=${deploymentId}`
    expect(counts).toEqual({ deletes: 0, removals: 1 })
  })

  it('concurrent workers claim each row once and a batch tail remains blocking', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    for (let index = 0; index < 30; index++) {
      const operation = { deploymentId, accountId: 'wealthfolio-account', idempotencyKey: `batch-${runId}-${index}`, subject: owner, accessGeneration: 1, kind: 'ActivityDelete', resourceId: `resource-${index}` }
      await sql`INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload) VALUES (${deploymentId},${owner},${connection.connectionId},${connection.providerGeneration},${operation.idempotencyKey},${sql.json(operation)})`
    }
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body)) as Record<string, unknown>
      const operation = 'activity_delete'
      const successor = null
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', operation, String(wire.resource_id), 'wealthfolio-account', '0', '', 'accepted', '0', '']
      return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner, idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation, resourceId: wire.resource_id, lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 3, receiptBinding: binding(fields), semanticHash: null }), { status: 200 })
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

  it('delivers a current-state row past an unrelated activity dead letter while keeping activity work fenced', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    const state = {
      accountId: 'wealthfolio-account', providerConnectionId: connection.connectionId,
      providerIncarnationId: sourceId, providerGeneration: 1,
      observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '100', liabilityBalance: '0', positions: [],
    } as const
    const currentState = currentStateToBridge(
      { deploymentId, localOwnerId: owner, sourceResourceIncarnationId: sourceId },
      state,
      currentStateSemanticHash(state),
      1,
      connection.connectionId,
    )
    const activity = {
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1,
      kind: 'ActivityUpsert' as const, resourceId: 'later-activity', idempotencyKey: 'later-activity-key',
      activity: {
        accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'POSTED',
        activityDate: '2026-07-18', activityDatePrecision: 'DATE' as const,
        amount: '5', currency: 'USD', sourceGroupId: 'provider-group',
      },
    }
    const deadLetter = {
      ...activity, resourceId: 'prime-saver-dead-letter', idempotencyKey: 'prime-saver-dead-letter-key',
      activity: { ...activity.activity, sourceGroupId: 'conflicting-provider-group' },
    }
    await sql`INSERT INTO projection_outbox
      (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state,attempts,last_error,dead_lettered_at,dead_letter_reason)
      VALUES (${deploymentId},${owner},${connection.connectionId},1,${deadLetter.idempotencyKey},${sql.json(deadLetter)},'dead_letter',3,'BRIDGE_HTTP_REJECTED',now(),'BRIDGE_HTTP_REJECTED')`
    await sql`INSERT INTO projection_outbox
      (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload)
      VALUES (${deploymentId},${owner},${connection.connectionId},1,${currentState.idempotencyKey},${sql.json(currentState as never)})`
    await sql`INSERT INTO projection_outbox
      (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload)
      VALUES (${deploymentId},${owner},${connection.connectionId},1,${activity.idempotencyKey},${sql.json(activity)})`

    const originalFetch = globalThis.fetch
    const calls: string[] = []
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body)) as Record<string, unknown>
      calls.push(String(wire.kind))
      expect(wire.kind).toBe('CurrentStateUpsert')
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', 'current_state_upsert', String(wire.resource_id), 'wealthfolio-account', '0', '', 'accepted', '1', currentState.semanticHash!]
      return new Response(JSON.stringify({
        deploymentId, accountId: 'wealthfolio-account', subject: owner,
        idempotencyKey: wire.idempotency_key, accessGeneration: 1,
        operation: 'current_state_upsert', resourceId: wire.resource_id,
        lifecycleSuccessorId: null, outcome: 'Accepted', bindingVersion: 3,
        receiptBinding: binding(fields), semanticHash: currentState.semanticHash,
      }), { status: 200 })
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      await expect(deps.projectPending(connection)).resolves.toEqual({ delivered: 1, failed: 2 })
      expect(calls).toEqual(['CurrentStateUpsert'])
      const rows = await sql`
        SELECT idempotency_key, state, dead_letter_reason
        FROM projection_outbox WHERE deployment_id=${deploymentId}`
      expect(rows).toEqual(expect.arrayContaining([
        { idempotency_key: deadLetter.idempotencyKey, state: 'dead_letter', dead_letter_reason: 'BRIDGE_HTTP_REJECTED' },
        { idempotency_key: currentState.idempotencyKey, state: 'delivered', dead_letter_reason: null },
        { idempotency_key: activity.idempotencyKey, state: 'pending', dead_letter_reason: null },
      ]))
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })
})

afterAll(async () => { await sql?.end() })
