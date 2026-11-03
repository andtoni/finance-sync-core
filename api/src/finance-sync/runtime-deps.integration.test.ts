import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { createRuntimeDependencies, retireUnmatchedInvestecCardAuthorizations } from './runtime-deps'
import { migrate } from './storage/migrate'
import { bridgeOperationToWire, currentStateToBridge } from './projection/bridge-client'
import { currentStateSemanticHash } from './projection/current-state'
import { assignProjectedRevision } from './projection/projected-revision'
import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import type { BridgeDisclosureOperation } from './projection/bridge-client'
import { runConfiguredSyncCycle, type RuntimeConnection } from './runtime'
import { PostgresOnboardingRepository } from './onboarding-repository'
import type { PlaidAccount } from './probes/plaid/plaid-adapter'
import type { ProviderTransportFactories } from './provider-runtime'
import type { FinanceSyncSql } from './storage/pg'

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

/**
 * Stand-in for the receiver's provider-write ordering: per written activity (a
 * lifecycle writes its successor) an unchanged revision must carry an unchanged
 * payload, a greater revision overwrites, and an older one is a no-op.
 */
function revisionCheckingBridge() {
  const held = new Map<string, { revision: number; activity: string }>()
  const rejected: string[] = []
  const handler = (async (_input, init) => {
    if (init?.method === 'GET') return new Response(null, { status: 404 })
    const wire = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
    const successor = typeof wire.lifecycle_successor_id === 'string' ? wire.lifecycle_successor_id : null
    const written = successor ?? String(wire.resource_id)
    const revision = Number(wire.observation_revision)
    const activity = JSON.stringify(wire.activity)
    const prior = held.get(written)
    if (prior && prior.revision === revision && prior.activity !== activity) {
      rejected.push(written)
      return new Response(null, { status: 400 })
    }
    if (!prior || revision > prior.revision) held.set(written, { revision, activity })
    const fields = [deploymentId, owner, String(wire.idempotency_key), '1', 'activity_upsert', String(wire.resource_id), 'wealthfolio-account', successor === null ? '0' : '1', successor ?? '', 'accepted', '0', '']
    return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner,
      idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation: 'activity_upsert',
      resourceId: wire.resource_id, lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 3,
      receiptBinding: binding(fields), semanticHash: null }), { status: 200 })
  }) as typeof fetch
  return { handler, held, rejected }
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

  it('reads the transactions an account holds under an earlier provider account id', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const held = (transactionId: string, accountId: string, date: string, amount: string, name: string): ObservationOutput => ({
      ...observation(`reissue-${transactionId}`, { transactionId: `reissue-${transactionId}`, accountId: 'provider-account', amount, currency: 'USD', date, name }),
      rawPayload: { transaction_id: `reissue-${transactionId}`, account_id: accountId },
    })
    await deps.persistAndEnqueue(connection, [
      held('old-1', 'account-old', '2026-09-05', '12.5', 'COFFEE'),
      held('old-2', 'account-old', '2026-09-06', '-40', 'REFUND'),
      held('new-1', 'account-new', '2026-09-20', '7', 'LUNCH'),
      // A removal of an old transaction carries no date or amount and must not hide it.
      { ...observation('reissue-old-1', { transactionId: 'reissue-old-1', isRemoved: true }), semanticHash: 'c'.repeat(64) },
    ], { skipCurrentState: true })
    const rows = await deps.loadPlaidReissuedHistory!(connection as Extract<RuntimeConnection, { provider: 'plaid' }>, 'account-new')
    expect([...rows].filter(row => ['COFFEE', 'REFUND', 'LUNCH'].includes(row.name)).sort((a, b) => a.date.localeCompare(b.date))).toEqual([
      { date: '2026-09-05', amount: '12.5', name: 'COFFEE' },
      { date: '2026-09-06', amount: '-40', name: 'REFUND' },
    ])
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

  it('delivers a backlog larger than one batch in a single pass and stops at the first failed delivery', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    for (let index = 0; index < 60; index++) {
      const operation = { deploymentId, accountId: 'wealthfolio-account', idempotencyKey: `backlog-${runId}-${index}`, subject: owner, accessGeneration: 1, kind: 'ActivityDelete', resourceId: `resource-${index}` }
      await sql`INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload) VALUES (${deploymentId},${owner},${connection.connectionId},${connection.providerGeneration},${operation.idempotencyKey},${sql.json(operation)})`
    }
    const originalFetch = globalThis.fetch
    let requests = 0
    let failFrom = Infinity
    globalThis.fetch = (async (_input, init) => {
      requests += 1
      if (requests >= failFrom) return new Response('', { status: 503 })
      const wire = JSON.parse(String(init?.body)) as Record<string, unknown>
      const operation = 'activity_delete'
      const successor = null
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', operation, String(wire.resource_id), 'wealthfolio-account', '0', '', 'accepted', '0', '']
      return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner, idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation, resourceId: wire.resource_id, lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 3, receiptBinding: binding(fields), semanticHash: null }), { status: 200 })
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      failFrom = 31
      await expect(deps.projectPending(connection)).resolves.toEqual({ delivered: 30, failed: 30 })
      // The failed row ends its batch's pass; nothing after that batch is attempted.
      expect(requests).toBeLessThanOrEqual(50)
      failFrom = Infinity
      await expect(deps.projectPending(connection)).resolves.toEqual({ delivered: 30, failed: 0 })
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('retires a recent hold whose merchant posted a different final amount, in real PostgreSQL', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    const day = (offset: number) => new Date(Date.now() + offset * 86_400_000).toISOString().slice(0, 10)
    const hold = {
      ...observation('grocer-hold', {
        transactionId: 'grocer-hold', accountId: 'provider-account', amount: '-1380.00', currency: 'ZAR',
        originalAmount: '1380.00', originalCurrency: 'ZAR', transactionDate: day(-2), date: day(-2),
        description: 'SYNTHETIC GROCER', type: 'DEBIT', transactionType: 'CARD', cardNumber: 'TEST-CARD-0011',
        metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } }, status: 'pending',
      }), providerVersion: 'investec:card:grocer-hold',
    }
    const charge = {
      ...observation('grocer-posted', {
        transactionId: 'grocer-posted', accountId: 'provider-account', amount: '-1214.35', currency: 'ZAR',
        transactionDate: day(-1), date: day(-1), postingDate: day(-1), settlementDate: day(-1),
        description: 'SYNTHETIC GROCER SAMPLEVILLE ZA', type: 'DEBIT', transactionType: 'CARDPURCHASES',
        cardNumber: '400000xxxxxx0011', status: 'posted',
      }), providerVersion: 'investec:txn:grocer-posted',
    }
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    await deps.persistAndEnqueue(investecConnection, [hold, charge], { skipCurrentState: true })
    const withoutDelivery = { ...deps, projectPending: async () => ({ delivered: 0, failed: 0 }) }
    await expect(retireUnmatchedInvestecCardAuthorizations(sql, deploymentId, investecConnection, withoutDelivery)).resolves.toBe(1)
    const [retirement] = await sql`
      SELECT payload->'activity'->>'status' AS status
      FROM projection_outbox
      WHERE deployment_id = ${deploymentId} AND payload->>'resourceId' LIKE '%:grocer-hold'
      ORDER BY created_at DESC LIMIT 1`
    expect(retirement.status).toBe('VOID')
    // The retirement is the hold's latest fact, so it is not considered again.
    await expect(retireUnmatchedInvestecCardAuthorizations(sql, deploymentId, investecConnection, withoutDelivery)).resolves.toBe(0)
    // A replay of the original pending observation must not bring the hold back.
    await deps.persistAndEnqueue(investecConnection, [hold], { skipCurrentState: true })
    const projected = await sql`
      SELECT payload->'activity'->>'status' AS status
      FROM projection_outbox
      WHERE deployment_id = ${deploymentId} AND payload->>'resourceId' LIKE '%:grocer-hold'
      ORDER BY created_at, (payload->>'observationRevision')::bigint`
    expect(projected.map(row => row.status)).toEqual(['PENDING', 'VOID'])
  })

  it('blocks a queued Investec predecessor after terminal lifecycle acceptance in real PostgreSQL', async () => {
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    const posted = {
      ...observation('terminal-posted', {
        transactionId: 'terminal-posted', accountId: 'provider-account', amount: '-10', currency: 'ZAR',
        transactionDate: '2026-07-17', date: '2026-07-17', postingDate: '2026-07-17', settlementDate: '2026-07-17',
      description: 'SYNTHETIC TERMINAL MERCHANT', reference: 'terminal-ref', type: 'DEBIT',
        transactionType: 'CARDPURCHASES', direction: 'DEBIT', status: 'posted',
      }), providerVersion: 'investec:txn:terminal-posted',
    }
    const card = {
      ...observation('terminal-card', {
        transactionId: 'terminal-card', accountId: 'provider-account', amount: '-10', currency: 'ZAR',
        transactionDate: '2026-07-17', originalAmount: '10', originalCurrency: 'ZAR', date: '2026-07-17',
      authorizationDateTime: '2026-07-17T10:00:00.000Z', description: 'SYNTHETIC TERMINAL MERCHANT',
      reference: 'terminal-ref', type: 'DEBIT', transactionType: 'CARD', cardNumber: 'TEST-CARD-0011',
        metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } },
        direction: 'DEBIT', status: 'pending',
      }), providerVersion: 'investec:card:terminal-card',
    }
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    await deps.persistAndEnqueue(investecConnection, [posted], { skipCurrentState: true })
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    await deps.persistAndEnqueue(investecConnection, [card], { skipCurrentState: true })
    const sourceResource = (id: string) => `${deploymentId}:${sourceId}:${id}`
    const [pending, lifecycle] = await sql<{ payload: BridgeDisclosureOperation; state: string }[]>`
      SELECT payload, state FROM projection_outbox
      WHERE deployment_id=${deploymentId} ORDER BY (payload ? 'lifecycleSuccessorId')
    `
    expect(pending.payload?.activity?.status).toBe('PENDING')
    expect(lifecycle.payload?.activity?.status).toBe('POSTED')
    expect(lifecycle.payload?.lifecycleSuccessorId).toBe(sourceResource('terminal-posted'))

    await sql`UPDATE projection_outbox SET state='suppressed', last_error='TEST_HOLD_PENDING'
      WHERE deployment_id=${deploymentId} AND idempotency_key=${pending.payload.idempotencyKey}`
    const [heldLifecycle] = await sql<{ state: string }[]>`
      SELECT state FROM projection_outbox
      WHERE deployment_id=${deploymentId} AND idempotency_key=${lifecycle.payload.idempotencyKey}`
    expect(heldLifecycle).toEqual({ state: 'pending' })
    const originalFetch = globalThis.fetch
    const calls: string[] = []
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body)) as Record<string, unknown>
      calls.push(String(wire.resource_id))
      const successor = lifecycle.payload.lifecycleSuccessorId ?? null
      const operation = 'activity_upsert'
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', operation, String(wire.resource_id), 'wealthfolio-account', successor === null ? '0' : '1', successor ?? '', 'accepted', '0', '']
      return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner, idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation, resourceId: wire.resource_id, lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 3, receiptBinding: binding(fields), semanticHash: null }), { status: 200 })
    }) as typeof fetch
    try {
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 1, failed: 0 })
      await sql`UPDATE projection_outbox SET state='pending', last_error=NULL
        WHERE deployment_id=${deploymentId} AND idempotency_key=${pending.payload.idempotencyKey}`
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 0 })
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 0 })
      expect(calls).toEqual([lifecycle.payload.resourceId])
      const [suppressed] = await sql`
        SELECT state, last_error FROM projection_outbox
        WHERE deployment_id=${deploymentId} AND idempotency_key=${pending.payload.idempotencyKey}`
      expect(suppressed).toEqual({ state: 'suppressed', last_error: 'INVESTEC_LIFECYCLE_ALREADY_TERMINAL' })
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('updates a delivered settlement as an ordinary posted successor when provider metadata changes', async () => {
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    const posted = { ...observation('revision-posted', {
      transactionId: 'revision-posted', accountId: 'provider-account', amount: '-10', currency: 'ZAR',
      transactionDate: '2026-07-17', date: '2026-07-17', postingDate: '2026-07-17', settlementDate: '2026-07-17',
      description: 'SYNTHETIC REVISION MERCHANT', reference: 'revision-ref', type: 'DEBIT',
      transactionType: 'CARDPURCHASES', direction: 'DEBIT', status: 'posted',
    }), providerVersion: 'investec:txn:revision-posted' }
    const card = { ...observation('revision-card', {
      transactionId: 'revision-card', accountId: 'provider-account', amount: '-10', currency: 'ZAR',
      transactionDate: '2026-07-17', originalAmount: '10', originalCurrency: 'ZAR', date: '2026-07-17',
      authorizationDateTime: '2026-07-17T10:00:00.000Z', description: 'SYNTHETIC REVISION MERCHANT',
      reference: 'revision-ref', type: 'DEBIT', transactionType: 'CARD', cardNumber: 'TEST-CARD-0011',
      metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } },
      direction: 'DEBIT', status: 'pending',
    }), providerVersion: 'investec:card:revision-card' }
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    await deps.persistAndEnqueue(investecConnection, [posted], { skipCurrentState: true })
    await deps.persistAndEnqueue(investecConnection, [card], { skipCurrentState: true })
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      const successor = typeof wire.lifecycle_successor_id === 'string' ? wire.lifecycle_successor_id : null
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', 'activity_upsert', String(wire.resource_id), 'wealthfolio-account', successor === null ? '0' : '1', successor ?? '', 'accepted', '0', '']
      return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner,
        idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation: 'activity_upsert',
        resourceId: wire.resource_id, lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 3,
        receiptBinding: binding(fields), semanticHash: null }), { status: 200 })
    }) as typeof fetch
    try {
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 3, failed: 0 })
      const [terminal] = await sql<{ payload: BridgeDisclosureOperation }[]>`SELECT payload FROM projection_outbox WHERE deployment_id=${deploymentId} AND payload->>'lifecycleSuccessorId' IS NOT NULL AND state='delivered'`
      await deps.persistAndEnqueue(investecConnection, [posted, card], { skipCurrentState: true })
      const [unchanged] = await sql<{ payload: BridgeDisclosureOperation }[]>`SELECT payload FROM projection_outbox WHERE deployment_id=${deploymentId} AND payload->>'resourceId'=${terminal.payload.lifecycleSuccessorId!} AND payload->>'lifecycleSuccessorId' IS NULL ORDER BY created_at DESC LIMIT 1`
      expect(unchanged.payload.activity).toEqual(terminal.payload.activity)
      expect(unchanged.payload.observationRevision).toBe(terminal.payload.observationRevision)
      expect(unchanged.payload.idempotencyKey).not.toBe(terminal.payload.idempotencyKey)
      expect(typeof bridgeOperationToWire(unchanged.payload).observation_revision).toBe('number')
      const revision = { ...posted, semanticHash: 'revision-posted-action-date', payload: {
        ...posted.payload, metadata: { providerOwned: { investec: { actionDate: '2026-07-18' } } },
      } }
      await expect(deps.persistAndEnqueue(investecConnection, [revision], { skipCurrentState: true })).resolves.toBe(1)
      const [ordinary] = await sql<{ payload: BridgeDisclosureOperation }[]>`SELECT payload FROM projection_outbox
        WHERE deployment_id=${deploymentId} AND payload->>'resourceId' LIKE ${`${deploymentId}:${sourceId}:revision-posted`}
          AND payload->>'lifecycleSuccessorId' IS NULL
        ORDER BY created_at DESC LIMIT 1`
      expect(ordinary.payload?.activity?.status).toBe('POSTED')
      const metadata = ordinary.payload?.activity?.metadata as Record<string, any> | undefined
      expect(metadata?.providerOwned?.investec?.actionDate).toBe('2026-07-18')
      expect(ordinary.payload?.lifecycleSuccessorId).toBeUndefined()
      expect(ordinary.payload?.resourceId).toBe(`${deploymentId}:${sourceId}:revision-posted`)
      expect(ordinary.payload?.resourceId).not.toBe(`${deploymentId}:${sourceId}:revision-card`)
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('keeps first-life and invalid/uncertain Investec deliveries distinct from terminal suppression', async () => {
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    const resource = (suffix: string) => `${deploymentId}:${sourceId}:${suffix}`
    const activity = (suffix: string, subject = owner): BridgeDisclosureOperation => ({
      deploymentId, accountId: 'wealthfolio-account', subject, accessGeneration: 1,
      idempotencyKey: `integration-${runId}-${suffix}`, kind: 'ActivityUpsert', resourceId: resource(suffix),
      activity: {
        accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'PENDING',
        activityDate: '2026-07-18T12:00:00.000Z', activityDatePrecision: 'INSTANT', amount: '1', currency: 'USD',
      },
    })
    const insert = async (payload: BridgeDisclosureOperation, state?: string) => {
      await sql`INSERT INTO projection_outbox
        (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${owner},${investecConnection.connectionId},1,${payload.idempotencyKey},${sql.json(payload as never)},${state ?? 'pending'})`
    }
    const originalFetch = globalThis.fetch
    const calls: string[] = []
    let mode: 'accepted' | 'invalid' | 'transport' = 'accepted'
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      calls.push(String(wire.resource_id ?? 'receipt-lookup'))
      if (init?.method === 'GET') return new Response(null, { status: 404 })
      if (mode === 'transport') {
        mode = 'accepted'
        throw new Error('synthetic transport uncertainty')
      }
      if (mode === 'invalid') return new Response(JSON.stringify({ outcome: 'Accepted' }), { status: 200 })
      const fields = [deploymentId, owner, String(wire.idempotency_key), '1', 'activity_upsert', String(wire.resource_id), 'wealthfolio-account', '0', '', 'accepted', '0', '']
      return new Response(JSON.stringify({ deploymentId, accountId: 'wealthfolio-account', subject: owner, idempotencyKey: wire.idempotency_key, accessGeneration: 1, operation: 'activity_upsert', resourceId: wire.resource_id, lifecycleSuccessorId: null, outcome: 'Accepted', bindingVersion: 3, receiptBinding: binding(fields), semanticHash: null }), { status: 200 })
    }) as typeof fetch
    try {
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
      const firstLife = activity('first-life')
      await insert(firstLife)
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 1, failed: 0 })
      expect(calls).toEqual([firstLife.resourceId])

      const invalid = activity('invalid-receipt')
      await insert(invalid)
      mode = 'invalid'
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 1 })
      const [uncertain] = await sql`SELECT state FROM projection_outbox WHERE deployment_id=${deploymentId} AND idempotency_key=${invalid.idempotencyKey}`
      expect(uncertain).toEqual({ state: 'uncertain' })

      mode = 'transport'
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 1 })
      const [stillUncertain] = await sql`SELECT state FROM projection_outbox WHERE deployment_id=${deploymentId} AND idempotency_key=${invalid.idempotencyKey}`
      expect(stillUncertain).toEqual({ state: 'uncertain' })
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 1, failed: 0 })

      const negative = activity('negative-identity')
      const wrongOwnerLifecycle: BridgeDisclosureOperation = {
        ...negative, subject: 'urn:other.test#owner', idempotencyKey: `integration-${runId}-wrong-owner`,
        lifecycleSuccessorId: resource('negative-posted'), activity: { ...negative.activity!, status: 'POSTED' },
      }
      await insert(wrongOwnerLifecycle, 'delivered')
      await insert(negative)
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 1, failed: 0 })
      expect(calls).toContain(negative.resourceId)
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('fences a pending predecessor when the terminal lifecycle receipt is unresolved', async () => {
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    const resource = (suffix: string) => `${deploymentId}:${sourceId}:${suffix}`
    const pending: BridgeDisclosureOperation = {
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1,
      kind: 'ActivityUpsert', resourceId: resource('uncertain-pending'), idempotencyKey: `integration-${runId}-uncertain-pending`,
      activity: { accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'PENDING',
        activityDate: '2026-07-18T12:00:00.000Z', activityDatePrecision: 'INSTANT', amount: '1', currency: 'USD' },
    }
    const lifecycle: BridgeDisclosureOperation = {
      ...pending, idempotencyKey: `integration-${runId}-uncertain-lifecycle`, lifecycleSuccessorId: resource('uncertain-posted'),
      activity: { ...pending.activity!, status: 'POSTED', activityDate: '2026-07-18', activityDatePrecision: 'DATE' },
    }
    const insert = async (payload: BridgeDisclosureOperation, state?: string) => {
      await sql`INSERT INTO projection_outbox
        (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${owner},${investecConnection.connectionId},1,${payload.idempotencyKey},${sql.json(payload as never)},${state ?? 'pending'})`
    }
    const originalFetch = globalThis.fetch
    const calls: string[] = []
    globalThis.fetch = (async (_input, init) => {
      calls.push(String(init?.method))
      if (init?.method === 'GET') return new Response(null, { status: 404 })
      throw new Error('synthetic lifecycle response loss')
    }) as typeof fetch
    try {
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      await insert(lifecycle)
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 1 })
      const [uncertainLifecycle] = await sql`SELECT state FROM projection_outbox WHERE deployment_id=${deploymentId} AND idempotency_key=${lifecycle.idempotencyKey}`
      expect(uncertainLifecycle).toEqual({ state: 'uncertain' })

      calls.length = 0
      await insert(pending)
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 2 })
      expect(calls).toEqual(['GET'])
      const [fencedPending] = await sql`
        SELECT state, last_error FROM projection_outbox
        WHERE deployment_id=${deploymentId} AND idempotency_key=${pending.idempotencyKey}`
      expect(fencedPending).toEqual({ state: 'uncertain', last_error: 'INVESTEC_LIFECYCLE_RECOVERY_REQUIRED' })
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('keeps an uncertain pending receipt unresolved after a delivered lifecycle', async () => {
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    const resource = (suffix: string) => `${deploymentId}:${sourceId}:${suffix}`
    const pending: BridgeDisclosureOperation = {
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1,
      kind: 'ActivityUpsert', resourceId: resource('accepted-pending'), idempotencyKey: `integration-${runId}-accepted-pending`,
      activity: { accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'PENDING',
        activityDate: '2026-07-18T12:00:00.000Z', activityDatePrecision: 'INSTANT', amount: '1', currency: 'USD' },
    }
    const lifecycle: BridgeDisclosureOperation = {
      ...pending, idempotencyKey: `integration-${runId}-delivered-lifecycle`, lifecycleSuccessorId: resource('accepted-posted'),
      activity: { ...pending.activity!, status: 'POSTED', activityDate: '2026-07-18', activityDatePrecision: 'DATE' },
    }
    const fields = [deploymentId, owner, pending.idempotencyKey, '1', 'activity_upsert', pending.resourceId, 'wealthfolio-account', '0', '', 'accepted', '0', '']
    const originalFetch = globalThis.fetch
    const calls: string[] = []
    globalThis.fetch = (async (_input, init) => {
      calls.push(String(init?.method))
      if (init?.method === 'POST') throw new Error('uncertain pending must not dispatch')
      return new Response(JSON.stringify({
        deploymentId, accountId: 'wealthfolio-account', subject: owner,
        idempotencyKey: pending.idempotencyKey, accessGeneration: 1,
        operation: 'activity_upsert', resourceId: pending.resourceId, lifecycleSuccessorId: null,
        outcome: 'Accepted', bindingVersion: 3, receiptBinding: binding(fields), semanticHash: null,
      }), { status: 200 })
    }) as typeof fetch
    try {
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
      await sql`INSERT INTO sync_checkpoints (deployment_id,connection_id,provider_generation,provider,cursor)
        VALUES (${deploymentId},${connection.connectionId},1,'investec','held-cursor')`
      await sql`INSERT INTO projection_outbox
        (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${owner},${investecConnection.connectionId},1,${lifecycle.idempotencyKey},${sql.json(lifecycle as never)},'delivered')`
      await sql`INSERT INTO projection_outbox
        (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${owner},${investecConnection.connectionId},1,${pending.idempotencyKey},${sql.json(pending as never)},'uncertain')`
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 0, failed: 1 })
      expect(calls).toEqual(['GET'])
      const [fencedPending] = await sql`
        SELECT state, last_error FROM projection_outbox
        WHERE deployment_id=${deploymentId} AND idempotency_key=${pending.idempotencyKey}`
      expect(fencedPending).toEqual({ state: 'uncertain', last_error: 'INVESTEC_LIFECYCLE_RECOVERY_REQUIRED' })
      const [checkpoint] = await sql`SELECT cursor FROM sync_checkpoints WHERE deployment_id=${deploymentId} AND connection_id=${connection.connectionId} AND provider_generation=1`
      expect(checkpoint).toEqual({ cursor: 'held-cursor' })
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
      await sql`DELETE FROM sync_checkpoints WHERE deployment_id=${deploymentId} AND connection_id=${connection.connectionId}`
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
      // The fenced activity stays pending and unresolved, so the cycle still
      // reports failure; the terminal activity dead letter itself no longer counts.
      await expect(deps.projectPending(connection)).resolves.toEqual({ delivered: 1, failed: 1 })
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
  it('reports cycle success when only terminal dead letters remain unresolved', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    const activity = (suffix: string) => ({
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1,
      kind: 'ActivityUpsert' as const, resourceId: `terminal-dead-letter-${suffix}`,
      idempotencyKey: `integration-${runId}-dead-${suffix}`,
      activity: {
        accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'POSTED',
        activityDate: '2026-07-18', activityDatePrecision: 'DATE' as const, amount: '5', currency: 'USD',
      },
    })
    for (const suffix of ['rejected-a', 'rejected-b']) {
      const row = activity(suffix)
      await sql`INSERT INTO projection_outbox
        (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state,attempts,last_error,dead_lettered_at,dead_letter_reason)
        VALUES (${deploymentId},${owner},${connection.connectionId},1,${row.idempotencyKey},${sql.json(row)},'dead_letter',3,'BRIDGE_HTTP_REJECTED',now(),'BRIDGE_HTTP_REJECTED')`
    }
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => { throw new Error('terminal dead letters must not be dispatched') }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      await expect(deps.projectPending(connection)).resolves.toEqual({ delivered: 0, failed: 0 })
      const [counts] = await sql`SELECT
        count(*) FILTER (WHERE state='dead_letter')::int dead_letters,
        count(*) FILTER (WHERE state='delivered')::int delivered
        FROM projection_outbox WHERE deployment_id=${deploymentId}`
      expect(counts).toEqual({ dead_letters: 2, delivered: 0 })
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('reports in-flight pending and uncertain rows as a cycle failure', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    const activity = (suffix: string) => ({
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1,
      kind: 'ActivityUpsert' as const, resourceId: `in-flight-${suffix}`,
      idempotencyKey: `integration-${runId}-in-flight-${suffix}`,
      activity: {
        accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'PENDING',
        activityDate: '2026-07-18T12:00:00.000Z', activityDatePrecision: 'INSTANT' as const,
        amount: '2', currency: 'USD',
      },
    })
    for (const state of ['pending', 'uncertain'] as const) {
      const row = activity(state)
      await sql`INSERT INTO projection_outbox
        (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${owner},${connection.connectionId},1,${row.idempotencyKey},${sql.json(row)},${state})`
    }
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (_input, init) => {
      if (init?.method === 'GET') return new Response(null, { status: 404 })
      throw new Error('synthetic transport uncertainty')
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      const result = await deps.projectPending(connection)
      expect(result.delivered).toBe(0)
      expect(result.failed).toBe(2)
      const [counts] = await sql`SELECT count(*) FILTER (WHERE state IN ('pending','uncertain'))::int in_flight
        FROM projection_outbox WHERE deployment_id=${deploymentId}`
      expect(counts).toEqual({ in_flight: 2 })
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('keeps a current-state dead letter as a full stop for the connection cycle', async () => {
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    const state = {
      accountId: 'wealthfolio-account', providerConnectionId: connection.connectionId,
      providerIncarnationId: sourceId, providerGeneration: 1,
      observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '100', liabilityBalance: '0', positions: [],
    } as const
    const currentState = currentStateToBridge(
      { deploymentId, localOwnerId: owner, sourceResourceIncarnationId: sourceId },
      state, currentStateSemanticHash(state), 1, connection.connectionId,
    )
    const activity = {
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1,
      kind: 'ActivityUpsert' as const, resourceId: 'gated-activity',
      idempotencyKey: `integration-${runId}-gated-activity`,
      activity: {
        accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'POSTED',
        activityDate: '2026-07-18', activityDatePrecision: 'DATE' as const, amount: '5', currency: 'USD',
      },
    }
    await sql`INSERT INTO projection_outbox
      (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state,attempts,last_error,dead_lettered_at,dead_letter_reason)
      VALUES (${deploymentId},${owner},${connection.connectionId},1,${currentState.idempotencyKey},${sql.json(currentState as never)},'dead_letter',3,'BRIDGE_HTTP_REJECTED',now(),'BRIDGE_HTTP_REJECTED')`
    await sql`INSERT INTO projection_outbox
      (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload)
      VALUES (${deploymentId},${owner},${connection.connectionId},1,${activity.idempotencyKey},${sql.json(activity)})`
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => { throw new Error('current-state dead letter must stop the cycle before dispatch') }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
      await expect(deps.projectPending(connection)).resolves.toEqual({ delivered: 0, failed: 1 })
      const [gate] = await sql`SELECT state FROM projection_outbox
        WHERE deployment_id=${deploymentId} AND idempotency_key=${activity.idempotencyKey}`
      expect(gate).toEqual({ state: 'pending' })
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('encodes a real PostgreSQL bigint revision as numeric uncorrelated wire JSON without precision loss', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const item = observation('numeric-wire', { transactionId: 'numeric-wire', accountId: 'provider-account', amount: '5', currency: 'USD', date: '2026-09-14', status: 'posted' })
    await deps.persistAndEnqueue(connection, [item], { skipCurrentState: true })
    const [ledger] = await sql`SELECT observation_revision FROM observation_revision_ledger WHERE deployment_id=${deploymentId} AND provider_version=${item.providerVersion}`
    expect(typeof ledger.observation_revision).toBe('string')
    const [row] = await sql<{ payload: BridgeDisclosureOperation }[]>`SELECT payload FROM projection_outbox WHERE deployment_id=${deploymentId} AND payload->>'resourceId'=${`${deploymentId}:${sourceId}:numeric-wire`}`
    expect(typeof row.payload.observationRevision).toBe('number')
    const wire = JSON.parse(JSON.stringify(bridgeOperationToWire(row.payload)))
    expect(wire.observation_revision).toBe(Number(ledger.observation_revision))
    expect(Number.isSafeInteger(wire.observation_revision)).toBe(true)
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
  })

  it('gives a settlement correlated after its own delivery a strictly greater revision', async () => {
    const investecConnection: RuntimeConnection = {
      ...connection, provider: 'investec', apiKey: 'test-api-key',
      eligibleProductNames: ['Private Bank Account'], fromDate: '2026-07-17', toDate: '2026-07-18',
    }
    // The authorization is observed first, so it holds the older ordinal. The
    // settled amount differs, so the two facts do not pair on their own.
    const card = { ...observation('late-link-card', {
      transactionId: 'late-link-card', accountId: 'provider-account', amount: '-10', currency: 'ZAR',
      transactionDate: '2026-07-17', originalAmount: '10', originalCurrency: 'ZAR', date: '2026-07-17',
      authorizationDateTime: '2026-07-17T10:00:00.000Z', description: 'SYNTHETIC LATE LINK MERCHANT',
      reference: 'late-link-ref', type: 'DEBIT', transactionType: 'CARD', cardNumber: 'TEST-CARD-0011',
      metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } },
      direction: 'DEBIT', status: 'pending',
    }), providerVersion: 'investec:card:late-link-card' }
    const posted = { ...observation('late-link-posted', {
      transactionId: 'late-link-posted', accountId: 'provider-account', amount: '-12', currency: 'ZAR',
      transactionDate: '2026-07-17', date: '2026-07-17', postingDate: '2026-07-17', settlementDate: '2026-07-17',
      description: 'SYNTHETIC LATE LINK MERCHANT', reference: 'late-link-ref', type: 'DEBIT',
      transactionType: 'CARDPURCHASES', direction: 'DEBIT', status: 'posted',
    }), providerVersion: 'investec:txn:late-link-posted' }
    const cardResource = `${deploymentId}:${sourceId}:late-link-card`
    const postedResource = `${deploymentId}:${sourceId}:late-link-posted`
    const written = () => sql<{ id: string; state: string; payload: BridgeDisclosureOperation }[]>`
      SELECT id, state, payload FROM projection_outbox
      WHERE deployment_id=${deploymentId}
        AND COALESCE(payload->>'lifecycleSuccessorId', payload->>'resourceId') IN (${cardResource}, ${postedResource})
      ORDER BY (payload->>'observationRevision')::bigint, (payload ? 'lifecycleSuccessorId'), payload->>'idempotencyKey'`
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const bridge = revisionCheckingBridge()
    const originalFetch = globalThis.fetch
    globalThis.fetch = bridge.handler
    try {
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
      await deps.persistAndEnqueue(investecConnection, [card], { skipCurrentState: true })
      await deps.persistAndEnqueue(investecConnection, [posted], { skipCurrentState: true })
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 2, failed: 0 })
      const [pendingCard, settledAlone] = await written()
      expect(pendingCard.payload.resourceId).toBe(cardResource)
      expect(settledAlone.payload.resourceId).toBe(postedResource)
      expect(settledAlone.payload.lifecycleSuccessorId).toBeUndefined()
      const deliveredRevision = settledAlone.payload.observationRevision!
      expect(pendingCard.payload.observationRevision!).toBeLessThan(deliveredRevision)

      // An operator link correlates the delivered settlement with the older
      // authorization. No provider fact changed, so no ledger ordinal did either.
      await sql`INSERT INTO investec_card_lifecycle_overrides
        (deployment_id, local_owner_id, source_resource_incarnation_id, pending_transaction_id, posted_transaction_id, actor_subject, reason)
        VALUES (${deploymentId}, ${owner}, ${sourceId}::uuid, 'late-link-card', 'late-link-posted', ${owner}, 'integration test')`
      await expect(deps.persistAndEnqueue(investecConnection, [posted], { skipCurrentState: true })).resolves.toBe(0)
      const correlatedRows = await written()
      const lifecycle = correlatedRows.find(row => row.payload.lifecycleSuccessorId === postedResource)!
      expect(lifecycle.payload.resourceId).toBe(cardResource)
      expect(lifecycle.payload.activity?.sourceType).toBe('CARD')
      expect(lifecycle.payload.activity).not.toEqual(settledAlone.payload.activity)
      expect(lifecycle.payload.observationRevision!).toBeGreaterThan(deliveredRevision)
      expect(Number.isSafeInteger(lifecycle.payload.observationRevision)).toBe(true)
      expect(lifecycle.payload.idempotencyKey).not.toBe(settledAlone.payload.idempotencyKey)
      const [{ ledger_max: ledgerMax }] = await sql<{ ledger_max: string }[]>`
        SELECT max(observation_revision) AS ledger_max FROM observation_revision_ledger WHERE deployment_id=${deploymentId}`
      expect(lifecycle.payload.observationRevision!).toBeGreaterThan(Number(ledgerMax))

      // Projecting the same correlated payload again is idempotent.
      await expect(deps.persistAndEnqueue(investecConnection, [posted], { skipCurrentState: true })).resolves.toBe(0)
      expect(await written()).toEqual(correlatedRows)

      const delivery = await deps.projectPending(investecConnection)
      expect(bridge.rejected).toEqual([])
      expect(delivery.failed).toBe(0)
      expect(bridge.held.get(postedResource)?.revision).toBe(lifecycle.payload.observationRevision)

      // After the lifecycle is terminal the same payload is an ordinary upsert
      // of the successor: a new operation at the unchanged revision.
      await deps.persistAndEnqueue(investecConnection, [posted], { skipCurrentState: true })
      const ordinary = (await written()).find(row => row.payload.resourceId === postedResource && row.state === 'pending')!
      expect(ordinary.payload.lifecycleSuccessorId).toBeUndefined()
      expect(ordinary.payload.activity).toEqual(lifecycle.payload.activity)
      expect(ordinary.payload.observationRevision).toBe(lifecycle.payload.observationRevision)
      await expect(deps.projectPending(investecConnection)).resolves.toEqual({ delivered: 1, failed: 0 })
      expect(bridge.rejected).toEqual([])
    } finally {
      globalThis.fetch = originalFetch
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('gives a changed derived payload of an unchanged observation a strictly greater revision', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const resourceId = `${deploymentId}:${sourceId}:derived-change`
    const item = observation('derived-change', {
      transactionId: 'derived-change', accountId: 'provider-account', amount: '5', currency: 'USD',
      date: '2026-09-14', status: 'posted', name: 'FIRST DERIVATION',
    })
    // Same observation identity, so the same raw row and ledger ordinal; only
    // the activity derived from it differs, as after a projection change.
    const rederived = { ...item, payload: { ...item.payload, name: 'SECOND DERIVATION' } }
    const rows = () => sql<{ payload: BridgeDisclosureOperation }[]>`
      SELECT payload FROM projection_outbox
      WHERE deployment_id=${deploymentId} AND payload->>'resourceId'=${resourceId}
      ORDER BY (payload->>'observationRevision')::bigint`
    await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    try {
      await deps.persistAndEnqueue(connection, [item], { skipCurrentState: true })
      const [first] = await rows()
      const ledger = await sql<{ observation_revision: string }[]>`
        SELECT observation_revision FROM observation_revision_ledger
        WHERE deployment_id=${deploymentId} AND provider_version=${item.providerVersion}`
      expect(ledger).toHaveLength(1)
      expect(first.payload.observationRevision).toBe(Number(ledger[0].observation_revision))

      await expect(deps.persistAndEnqueue(connection, [rederived], { skipCurrentState: true })).resolves.toBe(0)
      const afterChange = await rows()
      expect(afterChange).toHaveLength(2)
      const second = afterChange[1]
      expect(second.payload.activity?.notes).toBe('SECOND DERIVATION')
      expect(second.payload.observationRevision!).toBeGreaterThan(first.payload.observationRevision!)
      expect(second.payload.idempotencyKey).not.toBe(first.payload.idempotencyKey)
      expect(await sql`SELECT 1 FROM observation_revision_ledger
        WHERE deployment_id=${deploymentId} AND provider_version=${item.providerVersion}`).toHaveLength(1)

      // The same derived payload again keeps its revision and key.
      await deps.persistAndEnqueue(connection, [rederived], { skipCurrentState: true })
      expect(await rows()).toEqual(afterChange)

      // Returning to the earlier payload is a change again, never a stale replay.
      await deps.persistAndEnqueue(connection, [item], { skipCurrentState: true })
      const afterReturn = await rows()
      expect(afterReturn).toHaveLength(3)
      expect(afterReturn[2].payload.activity).toEqual(first.payload.activity)
      expect(afterReturn[2].payload.observationRevision!).toBeGreaterThan(second.payload.observationRevision!)
      expect(new Set(afterReturn.map(row => row.payload.idempotencyKey)).size).toBe(3)
      await deps.persistAndEnqueue(connection, [item], { skipCurrentState: true })
      expect(await rows()).toEqual(afterReturn)
    } finally {
      await sql`DELETE FROM projection_outbox WHERE deployment_id=${deploymentId}`
    }
  })

  it('serializes concurrent projections of one activity into distinct ordered revisions', async () => {
    const operation = (notes: string): BridgeDisclosureOperation => ({
      deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1, kind: 'ActivityUpsert',
      resourceId: `${deploymentId}:${sourceId}:concurrent-projection`, idempotencyKey: `concurrent-${notes}`, observationRevision: 1,
      activity: { accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'POSTED', activityDate: '2026-09-14',
        activityDatePrecision: 'DATE', amount: '5', currency: 'USD', notes },
    })
    // Both carry the same incoming ordinal and neither sees a prior payload.
    const assigned = await Promise.all(['left', 'right'].map(notes => sql.begin(async transaction => {
      const result = await assignProjectedRevision(transaction as unknown as FinanceSyncSql, operation(notes))
      await new Promise(resolveDelay => setTimeout(resolveDelay, 50))
      return result
    })))
    const [kept, raised] = assigned.slice().sort((a, b) => a.observationRevision! - b.observationRevision!)
    expect(kept.observationRevision).toBe(1)
    expect(kept.idempotencyKey).toMatch(/^concurrent-(left|right)$/)
    expect(raised.observationRevision!).toBeGreaterThan(1)
    expect(raised.idempotencyKey).not.toMatch(/^concurrent-/)
    expect(raised.activity?.notes).not.toBe(kept.activity?.notes)
  })

})

suite('Plaid Item with an account lacking a balance or not returned (PostgreSQL)', () => {
  it('ingests its transactions, emits no snapshot for it, and resumes a real snapshot once Plaid reports a balance', async () => {
    const itemDeployment = `${deploymentId}-null-balance`
    const itemId = `null-balance-item-${runId}`
    const digest = `sha256:${'a'.repeat(64)}`
    const repository = new PostgresOnboardingRepository(sql, itemDeployment)
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: itemId, secretReference: 'PLAID_NULL_BALANCE_SLOT',
      accounts: [
        { providerAccountId: 'acc-checking', name: 'Checking', type: 'checking', currency: 'USD', mask: '1111', assets: [] },
        { providerAccountId: 'acc-card', name: 'Card', type: 'credit card', currency: 'USD', mask: '2222', assets: [] },
      ],
    })
    for (const pending of await repository.listPending(owner, 'plaid')) {
      await repository.admit({
        subject: owner, provider: 'plaid', connectionId: pending.connectionId, providerAccountId: pending.providerAccountId,
        wealthfolioAccountId: `wf-${pending.providerAccountId}`, databaseStateDigest: digest,
        providerGeneration: pending.providerGeneration, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
      })
    }

    let checkingBalance: number | null = null
    let poll = 0
    const unexpected = () => { throw new Error('unexpected provider use') }
    const factories: ProviderTransportFactories = {
      investec: unexpected, mercury: unexpected, snaptrade: unexpected,
      plaid: () => ({
        getAccounts: async (): Promise<readonly PlaidAccount[]> => [
          { account_id: 'acc-checking', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: checkingBalance, iso_currency_code: 'USD' } },
          { account_id: 'acc-card', name: 'Card', type: 'credit', subtype: 'credit card', balances: { current: 200, iso_currency_code: 'USD' } },
        ],
        syncTransactions: async () => {
          poll += 1
          return {
            added: [
              { transaction_id: `checking-${poll}`, account_id: 'acc-checking', amount: 12.5, date: '2026-07-17', name: 'COFFEE' },
              { transaction_id: `card-${poll}`, account_id: 'acc-card', amount: 40, date: '2026-07-17', name: 'GROCER' },
            ],
            modified: [], removed: [], next_cursor: `cursor-${poll}`, has_more: false,
          }
        },
        getLiabilities: async () => ({ credit: [{ account_id: 'acc-card' }] }),
      }),
    }
    const deps = createRuntimeDependencies(sql, itemDeployment, {
      FINANCE_SYNC_BRIDGE_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_URL, FINANCE_SYNC_BRIDGE_RECEIPT_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_RECEIPT_URL,
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: runtimeEnv.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS, FINANCE_SYNC_BRIDGE_TOKEN: runtimeEnv.FINANCE_SYNC_BRIDGE_TOKEN,
    }, factories, undefined, { resolve: async () => ({ clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test-token' }) })
    const outbox = async () => await sql<{ account: string; kind: string; count: number }[]>`
      SELECT payload->>'accountId' AS account, payload->>'kind' AS kind, count(*)::int AS count
      FROM projection_outbox WHERE deployment_id = ${itemDeployment}
      GROUP BY 1, 2 ORDER BY 1, 2`
    const cursor = async () => (await sql<{ cursor: string | null }[]>`
      SELECT cursor FROM plaid_item_checkpoints WHERE deployment_id = ${itemDeployment} AND provider_connection_id = ${itemId}`)[0]?.cursor

    // Persist and enqueue only: bridge delivery is covered by the suite above.
    const controls = { providersEnabled: true, projectionEnabled: false }
    await expect(runConfiguredSyncCycle(itemDeployment, deps, controls)).resolves.toMatchObject({ connections: 2, failures: 0 })
    expect(await outbox()).toEqual([
      { account: 'wf-acc-card', kind: 'ActivityUpsert', count: 1 },
      { account: 'wf-acc-card', kind: 'CurrentStateUpsert', count: 1 },
      { account: 'wf-acc-checking', kind: 'ActivityUpsert', count: 1 },
    ])
    // No balance fact of any kind was stored for the account Plaid gave no balance for.
    const [stored] = await sql<{ transactions: number; balances: number }[]>`
      SELECT count(*) FILTER (WHERE provider_version LIKE 'plaid:txn:%')::int AS transactions,
        count(*) FILTER (WHERE payload ? 'currentBalance')::int AS balances
      FROM raw_observations WHERE deployment_id = ${itemDeployment} AND payload->>'accountId' = 'acc-checking'`
    expect(stored).toEqual({ transactions: 1, balances: 0 })
    expect(await cursor()).toBe('cursor-1')

    checkingBalance = 1000
    await expect(runConfiguredSyncCycle(itemDeployment, deps, controls)).resolves.toMatchObject({ connections: 2, failures: 0 })
    const [snapshot] = await sql<{ cash: string }[]>`
      SELECT payload->'currentState'->>'cashBalance' AS cash FROM projection_outbox
      WHERE deployment_id = ${itemDeployment} AND payload->>'accountId' = 'wf-acc-checking' AND payload->>'kind' = 'CurrentStateUpsert'`
    expect(snapshot).toEqual({ cash: '1000' })
    expect(await cursor()).toBe('cursor-2')
  })

  it('writes nothing for an account Plaid stops returning, keeps its sibling syncing on a held cursor, and catches up when it returns', async () => {
    const itemDeployment = `${deploymentId}-not-returned`
    const itemId = `not-returned-item-${runId}`
    const digest = `sha256:${'a'.repeat(64)}`
    const repository = new PostgresOnboardingRepository(sql, itemDeployment)
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: itemId, secretReference: 'PLAID_NOT_RETURNED_SLOT',
      accounts: [
        { providerAccountId: 'acc-checking', name: 'Checking', type: 'checking', currency: 'USD', mask: '1111', assets: [] },
        { providerAccountId: 'acc-card', name: 'Card', type: 'credit card', currency: 'USD', mask: '2222', assets: [] },
      ],
    })
    for (const pending of await repository.listPending(owner, 'plaid')) {
      await repository.admit({
        subject: owner, provider: 'plaid', connectionId: pending.connectionId, providerAccountId: pending.providerAccountId,
        wealthfolioAccountId: `wf-${pending.providerAccountId}`, databaseStateDigest: digest,
        providerGeneration: pending.providerGeneration, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
      })
    }

    let checkingReturned = false
    const requestedCursors: (string | undefined)[] = []
    const unexpected = () => { throw new Error('unexpected provider use') }
    const factories: ProviderTransportFactories = {
      investec: unexpected, mercury: unexpected, snaptrade: unexpected,
      plaid: () => ({
        getAccounts: async (): Promise<readonly PlaidAccount[]> => [
          ...(checkingReturned ? [{ account_id: 'acc-checking', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 1000, iso_currency_code: 'USD' } }] : []),
          { account_id: 'acc-card', name: 'Card', type: 'credit', subtype: 'credit card', balances: { current: 200, iso_currency_code: 'USD' } },
        ],
        // The stream is a pure function of the cursor, as Plaid's is: a held cursor replays the same rows.
        syncTransactions: async (_accessToken, cursor) => {
          requestedCursors.push(cursor)
          return {
            added: cursor ? [] : [
              { transaction_id: 'checking-1', account_id: 'acc-checking', amount: 12.5, date: '2026-07-17', name: 'COFFEE', iso_currency_code: 'USD' },
              { transaction_id: 'card-1', account_id: 'acc-card', amount: 40, date: '2026-07-17', name: 'GROCER', iso_currency_code: 'USD' },
            ],
            modified: [], removed: [], next_cursor: 'cursor-1', has_more: false,
          }
        },
        getLiabilities: async () => ({ credit: [{ account_id: 'acc-card' }] }),
      }),
    }
    const deps = createRuntimeDependencies(sql, itemDeployment, {
      FINANCE_SYNC_BRIDGE_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_URL, FINANCE_SYNC_BRIDGE_RECEIPT_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_RECEIPT_URL,
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: runtimeEnv.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS, FINANCE_SYNC_BRIDGE_TOKEN: runtimeEnv.FINANCE_SYNC_BRIDGE_TOKEN,
    }, factories, undefined, { resolve: async () => ({ clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test-token' }) })
    const stored = async () => (await sql<{ checking_rows: number; card_transactions: number; card_activities: number; checking_outbox: number; cursors: number }[]>`
      SELECT
        (SELECT count(*)::int FROM raw_observations WHERE deployment_id = ${itemDeployment} AND payload->>'accountId' = 'acc-checking') AS checking_rows,
        (SELECT count(*)::int FROM raw_observations WHERE deployment_id = ${itemDeployment} AND provider_version = 'plaid:txn:card-1') AS card_transactions,
        (SELECT count(*)::int FROM projection_outbox WHERE deployment_id = ${itemDeployment} AND payload->>'accountId' = 'wf-acc-card' AND payload->>'kind' = 'ActivityUpsert') AS card_activities,
        (SELECT count(*)::int FROM projection_outbox WHERE deployment_id = ${itemDeployment} AND payload->>'accountId' = 'wf-acc-checking') AS checking_outbox,
        (SELECT count(*)::int FROM plaid_item_checkpoints WHERE deployment_id = ${itemDeployment}) AS cursors`)[0]

    const controls = { providersEnabled: true, projectionEnabled: false }
    const condition = { failures: 0, plaidItemConditions: [{ providerConnectionId: itemId, reason: 'ACCOUNT_NOT_RETURNED' }] }
    await expect(runConfiguredSyncCycle(itemDeployment, deps, controls)).resolves.toMatchObject(condition)
    // The sibling is ingested; the missing account has no observation, no outbox row, and the cursor is not stored.
    expect(await stored()).toEqual({ checking_rows: 0, card_transactions: 1, card_activities: 1, checking_outbox: 0, cursors: 0 })

    // The next cycle replays the same stream from the held cursor without duplicating the sibling's rows.
    await expect(runConfiguredSyncCycle(itemDeployment, deps, controls)).resolves.toMatchObject(condition)
    expect(await stored()).toEqual({ checking_rows: 0, card_transactions: 1, card_activities: 1, checking_outbox: 0, cursors: 0 })

    checkingReturned = true
    await expect(runConfiguredSyncCycle(itemDeployment, deps, controls)).resolves.toMatchObject({ failures: 0, plaidItemConditions: [] })
    const [caughtUp] = await sql<{ kinds: string[] }[]>`
      SELECT array_agg(payload->>'kind' ORDER BY payload->>'kind') AS kinds FROM projection_outbox
      WHERE deployment_id = ${itemDeployment} AND payload->>'accountId' = 'wf-acc-checking'`
    expect(caughtUp.kinds).toEqual(['ActivityUpsert', 'CurrentStateUpsert'])
    expect((await stored()).card_activities).toBe(1)
    const [checkpoint] = await sql<{ cursor: string | null }[]>`
      SELECT cursor FROM plaid_item_checkpoints WHERE deployment_id = ${itemDeployment} AND provider_connection_id = ${itemId}`
    expect(checkpoint).toEqual({ cursor: 'cursor-1' })
    expect(requestedCursors).toEqual([undefined, undefined, undefined])
  })

  it('still refuses a snapshot assembled without the account balance', async () => {
    const deps = createRuntimeDependencies(sql, deploymentId, runtimeEnv)
    const transactionId = `no-balance-${runId}`
    const transaction = observation(transactionId, { transactionId, accountId: 'provider-account', amount: '4', currency: 'USD', date: '2026-07-17' })
    await expect(deps.persistAndEnqueue(connection, [transaction])).rejects.toThrow(/incomplete current-state snapshot: balance/)
    const [counts] = await sql`SELECT count(*)::int AS observations FROM raw_observations
      WHERE deployment_id=${deploymentId} AND payload->>'transactionId'=${transactionId}`
    expect(counts).toEqual({ observations: 0 })
  })
})

suite('Plaid admission moved to a re-issued account id (PostgreSQL)', () => {
  it('keeps the source resource, history and activity ids, and syncs the account from the connection that returns it', async () => {
    const moveDeployment = `${deploymentId}-move`
    const deadItem = `move-dead-item-${runId}`
    const liveItem = `move-live-item-${runId}`
    const digest = `sha256:${'a'.repeat(64)}`
    const repository = new PostgresOnboardingRepository(sql, moveDeployment)
    const checking = { name: 'Everyday Checking', type: 'checking', currency: 'USD', mask: '0042', assets: [] }
    const savings = { providerAccountId: 'acc-savings', name: 'Everyday Savings', type: 'savings', currency: 'USD', mask: '9999', assets: [] }
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: deadItem, secretReference: 'PLAID_MOVE_DEAD_SLOT',
      accounts: [{ providerAccountId: 'acc-old', ...checking }],
    })
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: liveItem, secretReference: 'PLAID_MOVE_LIVE_SLOT', accounts: [savings],
    })
    for (const pending of await repository.listPending(owner, 'plaid')) {
      await repository.admit({
        subject: owner, provider: 'plaid', connectionId: pending.connectionId, providerAccountId: pending.providerAccountId,
        wealthfolioAccountId: `wf-${pending.providerAccountId}`, databaseStateDigest: digest,
        providerGeneration: pending.providerGeneration, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
      })
    }
    const connectionIds = Object.fromEntries((await sql<{ item: string; id: string }[]>`
      SELECT provider_connection_id AS item, connection_id::text AS id FROM provider_connections WHERE deployment_id = ${moveDeployment}`)
      .map(row => [row.item, row.id]))
    const dead = connectionIds[deadItem]!
    const live = connectionIds[liveItem]!
    const discovered = async (connectionId: string, providerAccountId: string) => (await sql<{ source: string }[]>`
      SELECT source_resource_incarnation_id::text AS source FROM discovered_provider_accounts
      WHERE deployment_id = ${moveDeployment} AND connection_id = ${connectionId}::uuid AND provider_account_id = ${providerAccountId}`)[0]?.source
    const source = (await discovered(dead, 'acc-old'))!

    let reissued = false
    const requests: string[] = []
    const transaction = (transaction_id: string, account_id: string, date: string, amount: number, name: string) =>
      ({ transaction_id, account_id, amount, date, name, iso_currency_code: 'USD' })
    const page = (added: ReturnType<typeof transaction>[], next_cursor: string, removed: { transaction_id: string }[] = []) =>
      ({ added, modified: [], removed, next_cursor, has_more: false })
    const unexpected = () => { throw new Error('unexpected provider use') }
    const factories: ProviderTransportFactories = {
      investec: unexpected, mercury: unexpected, snaptrade: unexpected,
      plaid: () => ({
        getAccounts: async (accessToken): Promise<readonly PlaidAccount[]> => {
          requests.push(accessToken)
          if (accessToken === 'token-dead') {
            if (reissued) throw Object.assign(new Error('plaid request failed'), { code: 'NO_ACCOUNTS', errorType: 'ITEM_ERROR' })
            return [{ account_id: 'acc-old', name: 'Everyday Checking', mask: '0042', type: 'depository', subtype: 'checking', balances: { current: 5, iso_currency_code: 'USD' } }]
          }
          return [
            { account_id: 'acc-savings', name: 'Everyday Savings', mask: '9999', type: 'depository', subtype: 'savings', balances: { current: 100, iso_currency_code: 'USD' } },
            ...(reissued ? [{ account_id: 'acc-new', name: 'Everyday Checking', mask: '0042', type: 'depository', subtype: 'checking', balances: { current: 5, iso_currency_code: 'USD' } }] : []),
          ]
        },
        // Each stream is a pure function of its cursor, as Plaid's is.
        syncTransactions: async (accessToken, cursor) => {
          if (accessToken === 'token-dead') {
            return cursor ? page([], 'dead-1') : page([
              transaction('old-1', 'acc-old', '2026-08-01', 5, 'OLD'), transaction('old-2', 'acc-old', '2026-09-05', 12.5, 'COFFEE'),
            ], 'dead-1')
          }
          const history = transaction('new-old-1', 'acc-new', '2026-08-01', 5, 'OLD')
          const later = transaction('new-2', 'acc-new', '2026-09-21', 9, 'DINNER')
          if (!cursor) {
            return page([transaction('savings-1', 'acc-savings', '2026-07-17', 3, 'INTEREST'), ...(reissued ? [history, later] : [])], reissued ? 'cursor-3' : 'cursor-1')
          }
          if (cursor === 'cursor-1' && reissued) return page([history, transaction('new-1', 'acc-new', '2026-09-20', 7, 'LUNCH')], 'cursor-2')
          if (cursor === 'cursor-2') return page([later], 'cursor-3', [{ transaction_id: 'new-1' }])
          return page([], cursor)
        },
        getLiabilities: async () => ({}),
      }),
    }
    const deps = createRuntimeDependencies(sql, moveDeployment, {
      FINANCE_SYNC_BRIDGE_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_URL, FINANCE_SYNC_BRIDGE_RECEIPT_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_RECEIPT_URL,
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: runtimeEnv.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS, FINANCE_SYNC_BRIDGE_TOKEN: runtimeEnv.FINANCE_SYNC_BRIDGE_TOKEN,
    }, factories, undefined, { resolve: async reference => ({
      clientId: 'test-client', clientSecret: 'test-secret', accessToken: reference === 'PLAID_MOVE_DEAD_SLOT' ? 'token-dead' : 'token-live',
    }) })
    const checkingTransactions = async () => (await sql<{ id: string; account: string }[]>`
      SELECT DISTINCT payload->>'transactionId' AS id, payload->>'accountId' AS account FROM raw_observations
      WHERE deployment_id = ${moveDeployment} AND source_resource_incarnation_id = ${source}::uuid AND provider_version LIKE 'plaid:txn:%'
      ORDER BY 1`).map(row => `${row.id}@${row.account}`)
    const checkingOutbox = async (connectionId: string) => (await sql<{ kind: string; resource: string }[]>`
      SELECT DISTINCT payload->>'kind' AS kind, payload->>'resourceId' AS resource FROM projection_outbox
      WHERE deployment_id = ${moveDeployment} AND connection_id = ${connectionId} AND payload->>'accountId' = 'wf-acc-old'
      ORDER BY 1, 2`).map(row => `${row.kind}:${row.resource.slice(`${moveDeployment}:${source}:`.length)}`)

    // Persist and enqueue only: bridge delivery is covered by the suite above.
    const controls = { providersEnabled: true, projectionEnabled: false }
    await expect(runConfiguredSyncCycle(moveDeployment, deps, controls)).resolves.toMatchObject({ connections: 2, failures: 0, plaidReissuedAccounts: [] })
    expect(await checkingTransactions()).toEqual(['old-1@acc-old', 'old-2@acc-old'])

    // The bank re-issues the account into the other Item, and its own Item stops returning anything.
    reissued = true
    await repository.updateConnectionAccounts({
      subject: owner, providerConnectionId: liveItem, secretReference: 'PLAID_MOVE_LIVE_SLOT', providerGeneration: 1,
      accounts: [savings, { providerAccountId: 'acc-new', ...checking }],
    })
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const observed = { wealthfolioAccountId: 'wf-acc-old', from: { connectionId: dead, providerAccountId: 'acc-old' }, to: { connectionId: live, providerAccountId: 'acc-new' } }
    try {
      await expect(runConfiguredSyncCycle(moveDeployment, deps, controls)).resolves.toMatchObject({
        connections: 2, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [observed],
      })
    } finally {
      error.mockRestore()
    }
    // The repeated history is not taken again; the new transaction is held under the admitted account id.
    expect(await checkingTransactions()).toEqual(['new-1@acc-old', 'old-1@acc-old', 'old-2@acc-old'])

    const target = { subject: owner, wealthfolioAccountId: 'wf-acc-old', connectionId: live, providerAccountId: 'acc-new' }
    // Refused while projections of the previous runtime connection are undelivered.
    await expect(repository.moveAdmission({ ...target, from: observed.from })).rejects.toThrow(/undelivered projections/)
    await sql`UPDATE projection_outbox SET state = 'delivered', delivered_at = now() WHERE deployment_id = ${moveDeployment}`
    // Refused without the cycle's observation, and for an account with another identity.
    await expect(repository.moveAdmission(target)).rejects.toThrow(/not returned under the requested provider account/)
    await expect(repository.moveAdmission({ ...target, providerAccountId: 'acc-savings', from: observed.from })).rejects.toThrow(/not discovered/)

    const moved = { wealthfolioAccountId: 'wf-acc-old', connectionId: live, providerAccountId: 'acc-new', providerGeneration: 1 }
    await expect(repository.moveAdmission({ ...target, from: observed.from })).resolves.toEqual(moved)
    // A repeat, with or without the observation, answers the same and changes nothing.
    await expect(repository.moveAdmission({ ...target, from: observed.from })).resolves.toEqual(moved)
    await expect(repository.moveAdmission(target)).resolves.toEqual(moved)
    expect(await discovered(live, 'acc-new')).toBe(source)
    expect(await discovered(dead, 'acc-old')).not.toBe(source)
    expect(await sql<{ account: string; active: boolean }[]>`
      SELECT provider_account_id AS account, revoked_at IS NULL AS active FROM provider_account_admissions
      WHERE deployment_id = ${moveDeployment} AND wealthfolio_account_id = 'wf-acc-old' ORDER BY 1`)
      .toEqual([{ account: 'acc-new', active: true }, { account: 'acc-old', active: false }])

    // The emptied Item is no longer requested; the account syncs from the Item that returns it.
    requests.length = 0
    await expect(runConfiguredSyncCycle(moveDeployment, deps, controls)).resolves.toMatchObject({
      connections: 2, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [],
    })
    expect(requests).toEqual(['token-live'])
    expect(await checkingTransactions()).toEqual(['new-1@acc-old', 'new-2@acc-new', 'old-1@acc-old', 'old-2@acc-old'])
    // The removal of a transaction taken before the move is recorded against the same source resource.
    const [removal] = await sql<{ account: string }[]>`
      SELECT payload->>'accountId' AS account FROM raw_observations
      WHERE deployment_id = ${moveDeployment} AND source_resource_incarnation_id = ${source}::uuid AND provider_version = 'plaid:removed:new-1'`
    expect(removal).toEqual({ account: 'acc-new' })
    // Activity ids continue the same series. No snapshot yet: the previous runtime connection may have written today's.
    expect(await checkingOutbox(`${live}:acc-new`)).toEqual(['ActivityUpsert:new-2'])
    expect(await checkingOutbox(`${dead}:acc-old`)).toEqual(['ActivityUpsert:new-1', 'ActivityUpsert:old-1', 'ActivityUpsert:old-2', 'CurrentStateUpsert:current-state:wf-acc-old'])

    // From the day after the move the new runtime connection writes the snapshot.
    await sql`UPDATE provider_account_admissions SET revoked_at = revoked_at - interval '2 days'
      WHERE deployment_id = ${moveDeployment} AND provider_account_id = 'acc-old'`
    await expect(runConfiguredSyncCycle(moveDeployment, deps, controls)).resolves.toMatchObject({ failures: 0 })
    expect(await checkingOutbox(`${live}:acc-new`)).toEqual(['ActivityUpsert:new-2', 'CurrentStateUpsert:current-state:wf-acc-old'])

    // A fact derived from history taken before the move still names the earlier account id.
    const [movedConnection] = (await deps.loadConnections(moveDeployment)).filter(item => item.accountIds[0] === 'acc-new')
    const derived = (accountId: string): ObservationOutput => ({
      deploymentId: moveDeployment, localOwnerId: owner, sourceResourceIncarnationId: source,
      providerVersion: 'plaid:txn:old-2', semanticHash: accountId.padEnd(64, 'd').slice(0, 64), hashSpecificationVersion: 1, domain: 'cash_ledger',
      payload: { transactionId: 'old-2', accountId, amount: '12.5', currency: 'USD', date: '2026-09-05', name: 'COFFEE' },
      observedAt: '2026-09-05T00:00:00.000Z',
    })
    await expect(deps.persistAndEnqueue(movedConnection!, [derived('acc-old')], { skipCurrentState: true })).resolves.toBe(1)
    await expect(deps.persistAndEnqueue(movedConnection!, [derived('acc-other')], { skipCurrentState: true })).rejects.toThrow(/outside the admitted connection scope/)

    // A replay of the Item from its origin repeats the history under the new id; none of it is taken again.
    await sql`DELETE FROM plaid_item_checkpoints WHERE deployment_id = ${moveDeployment} AND provider_connection_id = ${liveItem}`
    await expect(runConfiguredSyncCycle(moveDeployment, deps, controls)).resolves.toMatchObject({ failures: 0 })
    expect(await checkingTransactions()).toEqual(['new-1@acc-old', 'new-2@acc-new', 'old-1@acc-old', 'old-2@acc-new', 'old-2@acc-old'])
  })
})

suite('Plaid admission moved onto an account an abandoned admission used (PostgreSQL)', () => {
  it('replaces the abandoned admission and leaves what it observed with the account left behind', async () => {
    const moveDeployment = `${deploymentId}-move-abandoned`
    const digest = `sha256:${'a'.repeat(64)}`
    const repository = new PostgresOnboardingRepository(sql, moveDeployment)
    const checking = { name: 'Everyday Checking', type: 'checking', currency: 'USD', mask: '0042', assets: [] }
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: `abandoned-dead-item-${runId}`, secretReference: 'PLAID_ABANDONED_DEAD_SLOT',
      accounts: [{ providerAccountId: 'acc-old', ...checking }],
    })
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: `abandoned-live-item-${runId}`, secretReference: 'PLAID_ABANDONED_LIVE_SLOT',
      accounts: [
        { providerAccountId: 'acc-savings', name: 'Everyday Savings', type: 'savings', currency: 'USD', mask: '9999', assets: [] },
        { providerAccountId: 'acc-new', ...checking },
      ],
    })
    const rows = await sql<{ connection: string; account: string; source: string }[]>`
      SELECT connection_id::text AS connection, provider_account_id AS account, source_resource_incarnation_id::text AS source
      FROM discovered_provider_accounts WHERE deployment_id = ${moveDeployment}`
    const row = (account: string) => rows.find(item => item.account === account)!
    const admit = (account: string, wealthfolioAccountId: string) => repository.admit({
      subject: owner, provider: 'plaid', connectionId: row(account).connection, providerAccountId: account, wealthfolioAccountId,
      databaseStateDigest: digest, providerGeneration: 1, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
    })
    // The re-issued account was once admitted to another Wealthfolio account, observed one fact, and was abandoned.
    await admit('acc-new', 'wf-abandoned')
    await sql`
      INSERT INTO raw_observations (deployment_id, local_owner_id, observation_id, source_resource_incarnation_id,
        provider_version, semantic_hash, hash_specification_version, payload, observed_at)
      VALUES (${moveDeployment}, ${owner}, gen_random_uuid(), ${row('acc-new').source}::uuid,
        'plaid:balance:acc-new', ${'e'.repeat(64)}, 1, ${sql.json({ accountId: 'acc-new', currentBalance: '5', currency: 'USD' })}, now())`
    await sql`UPDATE provider_account_admissions SET revoked_at = now(), baseline_state = 'pending', baseline_verified_at = NULL, baseline_fingerprint = NULL
      WHERE deployment_id = ${moveDeployment} AND provider_account_id = 'acc-new'`
    await admit('acc-old', 'wf-checking')
    await admit('acc-savings', 'wf-savings')
    const live = row('acc-new').connection
    const from = { connectionId: row('acc-old').connection, providerAccountId: 'acc-old' }
    const target = { subject: owner, wealthfolioAccountId: 'wf-checking', connectionId: live, providerAccountId: 'acc-new', from }
    const queue = (state: string) => sql`
      INSERT INTO projection_outbox (deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload, state)
      VALUES (${moveDeployment}, ${owner}, ${`${live}:acc-new`}, 1, ${`abandoned-${state}`}, ${sql.json({ kind: 'ActivityUpsert', accountId: 'wf-abandoned' })}, ${state})`

    // Anything the abandoned admission left queued would be delivered under the new admission's authority.
    await queue('pending')
    await expect(repository.moveAdmission(target)).rejects.toThrow(/undelivered projections/)
    await sql`UPDATE projection_outbox SET state = 'delivered', delivered_at = now() WHERE deployment_id = ${moveDeployment}`

    await expect(repository.moveAdmission(target)).resolves.toEqual({
      wealthfolioAccountId: 'wf-checking', connectionId: live, providerAccountId: 'acc-new', providerGeneration: 1,
    })
    expect(await sql<{ account: string; wealthfolio: string; active: boolean }[]>`
      SELECT provider_account_id AS account, wealthfolio_account_id AS wealthfolio, revoked_at IS NULL AS active
      FROM provider_account_admissions WHERE deployment_id = ${moveDeployment} ORDER BY 1`).toEqual([
      { account: 'acc-new', wealthfolio: 'wf-checking', active: true },
      { account: 'acc-old', wealthfolio: 'wf-checking', active: false },
      { account: 'acc-savings', wealthfolio: 'wf-savings', active: true },
    ])
    // The admission keeps its own source resource; the abandoned observation went with the other one to the account left behind.
    const after = await sql<{ account: string; source: string; observations: number }[]>`
      SELECT dpa.provider_account_id AS account, dpa.source_resource_incarnation_id::text AS source,
        (SELECT count(*)::int FROM raw_observations o WHERE o.deployment_id = dpa.deployment_id
          AND o.source_resource_incarnation_id = dpa.source_resource_incarnation_id) AS observations
      FROM discovered_provider_accounts dpa
      WHERE dpa.deployment_id = ${moveDeployment} AND dpa.provider_account_id IN ('acc-new', 'acc-old') ORDER BY 1`
    expect(after).toEqual([
      { account: 'acc-new', source: row('acc-old').source, observations: 0 },
      { account: 'acc-old', source: row('acc-new').source, observations: 1 },
    ])
    // The moved admission is the only authority the runtime loads for the account.
    const unexpected = () => { throw new Error('unexpected provider use') }
    const deps = createRuntimeDependencies(sql, moveDeployment, {
      FINANCE_SYNC_BRIDGE_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_URL, FINANCE_SYNC_BRIDGE_RECEIPT_URL: runtimeEnv.FINANCE_SYNC_BRIDGE_RECEIPT_URL,
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: runtimeEnv.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS, FINANCE_SYNC_BRIDGE_TOKEN: runtimeEnv.FINANCE_SYNC_BRIDGE_TOKEN,
    }, { investec: unexpected, mercury: unexpected, snaptrade: unexpected, plaid: unexpected }, undefined,
    { resolve: async () => ({ clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test-token' }) })
    expect((await deps.loadConnections(moveDeployment)).map(item => [item.connectionId, item.sourceResourceIncarnationId, item.wealthfolioAccountIds]))
      .toContainEqual([`${live}:acc-new`, row('acc-old').source, { 'acc-new': 'wf-checking' }])
  })

  it('still refuses observations that no abandoned admission explains', async () => {
    const moveDeployment = `${deploymentId}-move-unexplained`
    const digest = `sha256:${'a'.repeat(64)}`
    const repository = new PostgresOnboardingRepository(sql, moveDeployment)
    const checking = { name: 'Everyday Checking', type: 'checking', currency: 'USD', mask: '0042', assets: [] }
    await repository.recordConnection({
      subject: owner, provider: 'plaid', providerConnectionId: `unexplained-item-${runId}`, secretReference: 'PLAID_UNEXPLAINED_SLOT',
      accounts: [{ providerAccountId: 'acc-old', ...checking }, { providerAccountId: 'acc-new', ...checking }],
    })
    const rows = await sql<{ connection: string; account: string; source: string }[]>`
      SELECT connection_id::text AS connection, provider_account_id AS account, source_resource_incarnation_id::text AS source
      FROM discovered_provider_accounts WHERE deployment_id = ${moveDeployment}`
    const row = (account: string) => rows.find(item => item.account === account)!
    const admit = (account: string) => repository.admit({
      subject: owner, provider: 'plaid', connectionId: row(account).connection, providerAccountId: account, wealthfolioAccountId: 'wf-checking',
      databaseStateDigest: digest, providerGeneration: 1, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
    })
    const observe = () => sql`
      INSERT INTO raw_observations (deployment_id, local_owner_id, observation_id, source_resource_incarnation_id,
        provider_version, semantic_hash, hash_specification_version, payload, observed_at)
      VALUES (${moveDeployment}, ${owner}, gen_random_uuid(), ${row('acc-new').source}::uuid,
        'plaid:balance:acc-new', ${'f'.repeat(64)}, 1, ${sql.json({ accountId: 'acc-new', currentBalance: '5', currency: 'USD' })}, now())`
    const target = {
      subject: owner, wealthfolioAccountId: 'wf-checking', connectionId: row('acc-new').connection, providerAccountId: 'acc-new',
      from: { connectionId: row('acc-old').connection, providerAccountId: 'acc-old' },
    }
    // Taken for this same Wealthfolio account under an earlier admission of the re-issued id.
    await admit('acc-new')
    await observe()
    await sql`UPDATE provider_account_admissions SET revoked_at = now(), baseline_state = 'pending', baseline_verified_at = NULL, baseline_fingerprint = NULL
      WHERE deployment_id = ${moveDeployment} AND provider_account_id = 'acc-new'`
    await admit('acc-old')
    await expect(repository.moveAdmission(target)).rejects.toThrow(/already holds observations/)
    // With no admission row at all.
    await sql`DELETE FROM provider_account_admissions WHERE deployment_id = ${moveDeployment} AND provider_account_id = 'acc-new'`
    await expect(repository.moveAdmission(target)).rejects.toThrow(/already holds observations/)
  })
})

afterAll(async () => { await sql?.end() })
