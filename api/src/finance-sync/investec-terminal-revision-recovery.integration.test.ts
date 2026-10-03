import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { randomUUID } from 'node:crypto'
import { migrate } from './storage/migrate'
import { createInvestecTerminalRevisionRecoveryControl, terminalRevisionPayloadHash } from './investec-terminal-revision-recovery'
import { createRuntimeDependencies } from './runtime-deps'
import type { BridgeDisclosureOperation, BridgeReceipt } from './projection/bridge-client'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
const deploymentId = `terminal-recovery-${process.pid}-${Date.now()}`
const subject = 'urn:wealthfolio:test#recovery-owner'
let sql: ReturnType<typeof postgres>
let runtimeSql: ReturnType<typeof postgres>

function binding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v3;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
}
function receipt(operation: BridgeDisclosureOperation): BridgeReceipt {
  const wire = operation.kind === 'ActivityUpsert' ? 'activity_upsert' : 'activity_delete'
  const successor = operation.lifecycleSuccessorId ?? null
  const fields = [deploymentId, subject, operation.idempotencyKey, String(operation.accessGeneration), wire, operation.resourceId,
    operation.accountId, successor ? '1' : '0', successor ?? '', 'accepted', '0', '']
  return { deploymentId, accountId: operation.accountId, subject, idempotencyKey: operation.idempotencyKey,
    accessGeneration: operation.accessGeneration, operation: wire, resourceId: operation.resourceId,
    lifecycleSuccessorId: successor, outcome: 'Accepted', bindingVersion: 3, receiptBinding: binding(fields), semanticHash: null }
}
function lifecycle(id: string): BridgeDisclosureOperation {
  const source = randomUUID()
  return { deploymentId, accountId: `wealthfolio-recovery-${id}`, subject, accessGeneration: 1, kind: 'ActivityUpsert',
    resourceId: `${deploymentId}:${source}:pending-${id}`, lifecycleSuccessorId: `${deploymentId}:${source}:posted-${id}`,
    idempotencyKey: `lifecycle-${id}`, observationRevision: 2, activity: { accountId: `wealthfolio-recovery-${id}`, activityType: 'WITHDRAWAL', status: 'POSTED',
      activityDate: '2026-09-14', activityDatePrecision: 'DATE', amount: '1', currency: 'USD', metadata: { providerOwned: { investec: { actionDate: '2026-09-15' } } } } }
}
function priorLifecycle(op: BridgeDisclosureOperation): BridgeDisclosureOperation {
  return { ...op, idempotencyKey: `accepted-${op.idempotencyKey}`, observationRevision: 1,
    activity: { ...op.activity!, metadata: { providerOwned: { investec: { actionDate: '2026-09-14' } } } } }
}
async function seed(operation: BridgeDisclosureOperation, state: 'dead_letter'|'uncertain'|'pending'|'delivered' = 'dead_letter') {
  const connectionId = randomUUID()
  const principal = randomUUID(), account = randomUUID(), source = operation.resourceId.slice(deploymentId.length + 1).split(':')[0], incarnation = randomUUID()
  await sql`INSERT INTO provider_principals (deployment_id,local_owner_id,provider,provider_principal_id,provider_subject_id)
    VALUES (${deploymentId},${subject},'investec',${principal},${principal})`
  await sql`INSERT INTO connection_incarnations (deployment_id,local_owner_id,connection_incarnation_id,provider_principal_id,provider_connection_id,incarnation)
    VALUES (${deploymentId},${subject},${incarnation},${principal},${connectionId},1)`
  await sql`INSERT INTO account_incarnations (deployment_id,local_owner_id,account_incarnation_id,connection_incarnation_id,provider_account_id,incarnation)
    VALUES (${deploymentId},${subject},${account},${incarnation},'provider-account',1)`
  await sql`INSERT INTO resource_incarnations (deployment_id,local_owner_id,source_resource_incarnation_id,account_incarnation_id,resource_kind,provider_resource_id,incarnation)
    VALUES (${deploymentId},${subject},${source},${account},'cash_ledger','resource',1)`
  await sql`INSERT INTO provider_connections (deployment_id,local_owner_id,connection_id,provider_principal_id,provider,provider_connection_id,secret_reference,state)
    VALUES (${deploymentId},${subject},${connectionId},${principal},'investec',${connectionId},'test-secret','active')`
  await sql`INSERT INTO discovered_provider_accounts (deployment_id,local_owner_id,connection_id,provider_account_id,account_incarnation_id,source_resource_incarnation_id,provider_name,provider_type,currency,discovery_generation)
    VALUES (${deploymentId},${subject},${connectionId},'provider-account',${account},${source},'test','checking','USD',1)`
  await sql`INSERT INTO provider_account_admissions (deployment_id,local_owner_id,connection_id,provider_account_id,wealthfolio_account_id,receiver_access_generation,asset_mapping_digest,baseline_state,baseline_verified_at,baseline_fingerprint,provider_generation)
    VALUES (${deploymentId},${subject},${connectionId},'provider-account',${operation.accountId},1,${'sha256:' + 'a'.repeat(64)},'verified',now(),'test',1)`
  const l1 = priorLifecycle(operation)
  await sql`INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state,created_at)
    VALUES (${deploymentId},${subject},${connectionId + ':provider-account'},1,${l1.idempotencyKey},${sql.json(l1 as never)},'delivered',now()-interval '1 minute')`
  const cardId = operation.resourceId.slice(`${deploymentId}:${source}:`.length)
  const settledId = operation.lifecycleSuccessorId!.slice(`${deploymentId}:${source}:`.length)
  const card = { transactionId: cardId, providerLifecycleTransactionId: settledId, accountId: 'provider-account', amount: '1', currency: 'USD', status: 'pending', date: '2026-09-14', transactionDate: '2026-09-14', description: 'Synthetic merchant', sourceType: 'CARD' }
  const settled = { transactionId: settledId, providerLifecycleTransactionId: cardId, accountId: 'provider-account', amount: '1', currency: 'USD', status: 'posted', transactionType: 'CARDPURCHASES', date: '2026-09-14', transactionDate: '2026-09-14', settlementDate: '2026-09-14', description: 'Synthetic merchant' }
  for (const [version, hash, payload] of [
    [`investec:card:${cardId}`, 'a'.repeat(64), card],
    [`investec:txn:${settledId}`, 'b'.repeat(64), { ...settled, metadata: { providerOwned: { investec: { actionDate: '2026-09-15' } } } }],
    [`investec:txn:${settledId}`, 'c'.repeat(64), { ...settled, metadata: { providerOwned: { investec: { actionDate: '2026-09-16' } } } }],
  ] as const) {
    await sql`INSERT INTO raw_observations (observation_id,deployment_id,local_owner_id,source_resource_incarnation_id,provider_version,semantic_hash,hash_specification_version,payload,observed_at)
      VALUES (gen_random_uuid(),${deploymentId},${subject},${source},${version},${hash},1,${sql.json(payload as never)},now())`
    await sql`INSERT INTO observation_revision_ledger (deployment_id,local_owner_id,source_resource_incarnation_id,provider_version,semantic_hash,hash_specification_version,observation_revision)
      VALUES (${deploymentId},${subject},${source},${version},${hash},1,nextval('finance_sync_observation_revision_seq'))`
  }
  const [row] = await sql<{ id: string; payload: BridgeDisclosureOperation }[]>`
    INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state,last_error)
    VALUES (${deploymentId},${subject},${connectionId + ':provider-account'},1,${operation.idempotencyKey},${sql.json(operation as never)},${state},'original bridge error') RETURNING id,payload`
  return { id: row.id, operation: row.payload, connectionId, source, l1 }
}

function ordinaryOperation(op: BridgeDisclosureOperation): BridgeDisclosureOperation {
  return { ...op, resourceId: op.lifecycleSuccessorId!, lifecycleSuccessorId: undefined, idempotencyKey: `ordinary-${op.idempotencyKey}` }
}
function requestFor(seeded: Awaited<ReturnType<typeof seed>>) {
  return { outboxId: seeded.id, expectedPayloadHash: terminalRevisionPayloadHash(seeded.operation), reason: 'security recovery', apply: true }
}
function deferred() {
  let release!: () => void
  const promise = new Promise<void>(resolve => { release = resolve })
  return { promise, release }
}

suite('Investec terminal revision recovery against PostgreSQL', () => {
  beforeAll(async () => {
    sql = postgres(url!, { prepare: false })
    await sql`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='finance_sync_runtime') THEN CREATE ROLE finance_sync_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS; END IF; END $$`
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
    // Existing runtime tables are provisioned by deployment, not migration 0022.
    await sql`GRANT USAGE ON SCHEMA public TO finance_sync_runtime`
    await sql`GRANT SELECT, UPDATE ON provider_connections,discovered_provider_accounts,provider_account_admissions,projection_outbox TO finance_sync_runtime`
    await sql`GRANT SELECT ON investec_card_lifecycle_overrides,observation_quarantines TO finance_sync_runtime`
    runtimeSql = postgres(url!, { prepare: false, max: 1 })
    await runtimeSql`SET ROLE finance_sync_runtime`
  })
  afterAll(async () => { await runtimeSql?.end(); await sql?.end() })

  it('dry-run writes nothing, then apply claims, delivers, suppresses, audits, and preserves selected/nonselected rows', async () => {
    const op = lifecycle('success')
    const seeded = await seed(op)
    const id = seeded.id
    const opFromDb = seeded.operation
    const ordinary = { ...op, resourceId: op.lifecycleSuccessorId!, lifecycleSuccessorId: undefined, idempotencyKey: 'ordinary-success' }
    const resolver = {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinary, boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async (candidate: BridgeDisclosureOperation) => receipt(candidate),
    }
    const control = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, resolver)
    const hash = terminalRevisionPayloadHash(opFromDb)
    const before = await sql`SELECT count(*)::int AS intents FROM investec_terminal_revision_recovery_intents`
    const dry = await control.recover(subject, { outboxId: id, expectedPayloadHash: hash, reason: 'dry run check' })
    expect(dry.state).toBe('planned')
    expect((await sql`SELECT count(*)::int AS intents FROM investec_terminal_revision_recovery_intents`)[0].intents).toBe(before[0].intents)
    const [current] = await sql<{ payload: BridgeDisclosureOperation }[]>`SELECT payload FROM projection_outbox WHERE id=${id}::uuid`
    const appliedHash = terminalRevisionPayloadHash(current.payload)
    await sql`UPDATE projection_outbox SET state='dead_letter' WHERE id=${id}::uuid`
    const applied = await control.recover(subject, { outboxId: id, expectedPayloadHash: appliedHash, reason: 'reconcile terminal revision', apply: true })
    expect(applied.state).toBe('suppressed')
    expect((await sql`SELECT state,last_error,dead_letter_reason FROM projection_outbox WHERE id=${id}::uuid`)[0]).toEqual({ state: 'suppressed', last_error: 'original bridge error', dead_letter_reason: 'TERMINAL_REVISION_RECONCILED' })
    expect((await sql`SELECT count(*)::int AS audits FROM investec_terminal_revision_recovery_audit WHERE outbox_id=${id}::uuid`)[0].audits).toBe(1)
    const duplicate = await control.recover(subject, { outboxId: id, expectedPayloadHash: appliedHash, reason: 'duplicate', apply: true })
    expect(duplicate.idempotent).toBe(true)
    const other = lifecycle('nonselected')
    await seed(other)
    expect((await sql`SELECT state,last_error FROM projection_outbox WHERE deployment_id=${deploymentId} AND idempotency_key='lifecycle-nonselected'`)[0]).toEqual({ state: 'dead_letter', last_error: 'original bridge error' })
  })

  it('retries a lost receiver response with the same durable ordinary operation', async () => {
    const op = lifecycle('lost-response')
    const seeded = await seed(op)
    const ordinary = { ...op, resourceId: op.lifecycleSuccessorId!, lifecycleSuccessorId: undefined, idempotencyKey: 'ordinary-lost-response' }
    let deliveries = 0
    const resolver = {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinary, boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async (candidate: BridgeDisclosureOperation) => { deliveries++; if (deliveries === 1) throw new Error('response lost after receiver commit'); return receipt(candidate) },
    }
    const control = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, resolver)
    const hash = terminalRevisionPayloadHash(seeded.operation)
    await expect(control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: hash, reason: 'lost response', apply: true })).rejects.toThrow(/response lost/)
    const retried = await control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: hash, reason: 'retry lost response', apply: true })
    expect(retried.state).toBe('suppressed')
    expect(retried.operation?.idempotencyKey).toBe('ordinary-lost-response')
    expect(deliveries).toBe(2)
    expect((await sql`SELECT reason FROM investec_terminal_revision_recovery_audit WHERE outbox_id=${seeded.id}::uuid`)[0].reason).toBe('lost response')
  })

  it('gives a recovered successor payload a strictly greater revision than a different payload already projected at its revision', async () => {
    const op = lifecycle('revision-conflict')
    const seeded = await seed(op)
    const ordinary = ordinaryOperation(op)
    // The successor was last projected at this very revision with another payload.
    await sql`INSERT INTO projected_activity_revision_ledger (deployment_id,local_owner_id,resource_id,observation_revision,payload_digest)
      VALUES (${deploymentId},${subject},${ordinary.resourceId},${ordinary.observationRevision!},${'0'.repeat(64)})`
    const delivered: BridgeDisclosureOperation[] = []
    const control = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinary, boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async candidate => { delivered.push(candidate); return receipt(candidate) },
    })
    const applied = await control.recover(subject, requestFor(seeded))
    expect(applied.state).toBe('suppressed')
    expect(delivered).toHaveLength(1)
    expect(delivered[0].activity).toEqual(ordinary.activity)
    expect(delivered[0].observationRevision!).toBeGreaterThan(ordinary.observationRevision!)
    expect(delivered[0].idempotencyKey).not.toBe(ordinary.idempotencyKey)
    const [intent] = await sql`SELECT ordinary_operation,new_operation_idempotency_key FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${seeded.id}::uuid`
    expect(intent.ordinary_operation).toEqual(delivered[0])
    expect(intent.new_operation_idempotency_key).toBe(delivered[0].idempotencyKey)
    const [head] = await sql`SELECT observation_revision FROM projected_activity_revision_ledger
      WHERE deployment_id=${deploymentId} AND resource_id=${ordinary.resourceId} ORDER BY observation_revision DESC LIMIT 1`
    expect(Number(head.observation_revision)).toBe(delivered[0].observationRevision)
  })

  it('rejects concurrent duplicate claims and preserves a payload CAS race', async () => {
    const op = lifecycle('concurrent')
    const seeded = await seed(op)
    const gate = deferred()
    const entered = deferred()
    const ordinary = { ...op, resourceId: op.lifecycleSuccessorId!, lifecycleSuccessorId: undefined, idempotencyKey: 'ordinary-concurrent' }
    const resolver = {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinary, boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async (candidate: BridgeDisclosureOperation) => { entered.release(); await gate.promise; return receipt(candidate) },
    }
    const control = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, resolver)
    const hash = terminalRevisionPayloadHash(seeded.operation)
    const first = control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: hash, reason: 'concurrent one', apply: true })
    await entered.promise
    const second = control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: hash, reason: 'concurrent two', apply: true }).then(value => ({ ok: true as const, value }), reason => ({ ok: false as const, reason }))
    await second
    gate.release()
    const results = await Promise.all([first.then(value => ({ ok: true as const, value }), reason => ({ ok: false as const, reason })), second])
    expect(results.filter(result => result.ok)).toHaveLength(1)
    expect(results.filter(result => !result.ok && /already claimed/.test(String(result.reason)))).toHaveLength(1)

    const raced = lifecycle('payload-race')
    const racedSeed = await seed(raced)
    const racedControl = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(raced), ordinaryOperation: { ...raced, resourceId: raced.lifecycleSuccessorId!, lifecycleSuccessorId: undefined }, boundReceipt: receipt(priorLifecycle(raced)) }),
      deliver: async (candidate: BridgeDisclosureOperation) => receipt(candidate),
    })
    const wrongHash = terminalRevisionPayloadHash(racedSeed.operation)
    await sql`UPDATE projection_outbox SET payload=jsonb_set(payload,'{activity,notes}','"changed"') WHERE id=${racedSeed.id}::uuid`
    await expect(racedControl.recover(subject, { outboxId: racedSeed.id, expectedPayloadHash: wrongHash, reason: 'payload changed', apply: true })).rejects.toThrow(/CAS/)
  })

  it('rejects generation change before finalization and retains the durable claim', async () => {
    const op = lifecycle('generation-race')
    const seeded = await seed(op)
    const ordinary = { ...op, resourceId: op.lifecycleSuccessorId!, lifecycleSuccessorId: undefined, idempotencyKey: 'ordinary-generation-race' }
    const resolver = {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinary, boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async (candidate: BridgeDisclosureOperation) => { await sql`UPDATE projection_outbox SET provider_generation=2 WHERE id=${seeded.id}::uuid`; return receipt(candidate) },
    }
    const control = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, resolver)
    await expect(control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: terminalRevisionPayloadHash(seeded.operation), reason: 'generation revoked', apply: true })).rejects.toThrow(/changed/)
    expect((await sql`SELECT state FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0].state).toBe('dead_letter')
  })

  it('runs apply/retry as the restricted runtime role with the production resolver: validates bound L1 GET, GETs ordinary receipt before retry, and avoids a second POST', async () => {
    const op = lifecycle('production-resolver')
    const seeded = await seed(op)
    const ordinary = { ...op, resourceId: op.lifecycleSuccessorId!, lifecycleSuccessorId: undefined, idempotencyKey: 'ordinary-production-resolver' }
    const calls: string[] = []
    let ordinaryReceiptAvailable = false
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input)
      calls.push(`${init?.method ?? 'GET'} ${url}`)
      if (url.endsWith('/' + encodeURIComponent(op.idempotencyKey))) return new Response(null, { status: 404 })
      if (url.endsWith(encodeURIComponent(seeded.l1.idempotencyKey))) return new Response(JSON.stringify(receipt(seeded.l1)), { status: 200 })
      if (url.includes('/receipts/')) {
        if (ordinaryReceiptAvailable) {
          const key = decodeURIComponent(url.split('/').pop()!)
          return new Response(JSON.stringify(receipt({ ...ordinary, idempotencyKey: key })), { status: 200 })
        }
        return new Response(null, { status: 404 })
      }
      if (url.includes('/bridge')) {
        ordinaryReceiptAvailable = true
        throw new Error('lost response after receiver commit')
      }
      return new Response(null, { status: 404 })
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(runtimeSql, deploymentId, {
        FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
        FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
      })
      if (!deps.terminalRevisionResolver) throw new Error('production resolver was not constructed')
      const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, deps.terminalRevisionResolver)
      expect((await runtimeSql`SELECT current_user, rolsuper, rolcreaterole FROM pg_roles WHERE rolname=current_user`)[0]).toEqual({ current_user: 'finance_sync_runtime', rolsuper: false, rolcreaterole: false })
      const hash = terminalRevisionPayloadHash(seeded.operation)
      await expect(control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: hash, reason: 'production resolver retry', apply: true })).rejects.toThrow(/lost response/)
      const retry = await control.recover(subject, { outboxId: seeded.id, expectedPayloadHash: hash, reason: 'production resolver retry', apply: true })
      expect(retry.state).toBe('suppressed')
      expect(calls.filter(call => call.endsWith('/' + encodeURIComponent(op.idempotencyKey)))).toHaveLength(0)
      expect(calls.filter(call => call.endsWith('/' + encodeURIComponent(seeded.l1.idempotencyKey)))).toHaveLength(1)
      expect(retry.operation?.activity?.metadata).toMatchObject({ providerOwned: { investec: { actionDate: '2026-09-16' } } })
      expect(typeof retry.operation?.observationRevision).toBe('number')
      expect(calls.filter(call => call.startsWith('POST https://bridge.test/bridge'))).toHaveLength(1)
    } finally { globalThis.fetch = originalFetch }
  })

  it('fails closed on payload CAS and wrong authenticated subject without mutation', async () => {
    const op = lifecycle('negative')
    const seeded = await seed(op, 'dead_letter')
    const id = seeded.id
    const resolver = { resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: { ...op, lifecycleSuccessorId: undefined }, boundReceipt: receipt(priorLifecycle(op)) }), deliver: async () => receipt(op) }
    const control = createInvestecTerminalRevisionRecoveryControl(sql, deploymentId, resolver)
    await expect(control.recover(subject, { outboxId: id, expectedPayloadHash: 'f'.repeat(64), reason: 'wrong hash', apply: true })).rejects.toThrow(/CAS/)
    await expect(control.recover('urn:wealthfolio:test#other', { outboxId: id, expectedPayloadHash: terminalRevisionPayloadHash(seeded.operation), reason: 'wrong subject', apply: true })).rejects.toThrow(/CAS/)
    expect((await sql`SELECT state FROM projection_outbox WHERE id=${id}::uuid`)[0].state).toBe('dead_letter')
    expect((await sql`SELECT count(*)::int AS intents FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${id}::uuid`)[0].intents).toBe(0)
  })

  it.each(['pending', 'delivered', 'uncertain'] as const)('rejects %s before any network or claim', async state => {
    const seeded = await seed(lifecycle(`state-${state}`), state)
    let calls = 0
    const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
      resolve: async () => { calls++; throw new Error('unexpected network') },
      deliver: async () => { calls++; throw new Error('unexpected network') },
    })
    await expect(control.recover(subject, requestFor(seeded))).rejects.toThrow(/dead_letter/)
    expect(calls).toBe(0)
    expect((await sql`SELECT count(*)::int AS count FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${seeded.id}::uuid`)[0].count).toBe(0)
    expect((await sql`SELECT state FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0].state).toBe(state)
  })

  const authorityChanges = ['rotation', 'revocation', 'access-generation', 'admission-generation', 'unverified', 'discovery-generation', 'inactive'] as const
  async function changeAuthority(kind: typeof authorityChanges[number], connectionId: string) {
    switch (kind) {
      case 'rotation':
        await sql`UPDATE provider_connections SET provider_generation=provider_generation+1,secret_reference='rotated-test-secret' WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
        break
      case 'revocation':
        await sql`UPDATE provider_account_admissions SET revoked_at=now() WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
        break
      case 'access-generation':
        await sql`UPDATE provider_account_admissions SET receiver_access_generation=2 WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
        break
      case 'admission-generation':
        await sql`UPDATE provider_account_admissions SET provider_generation=2 WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
        break
      case 'unverified':
        await sql`UPDATE provider_account_admissions SET baseline_state='pending',baseline_verified_at=NULL WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
        break
      case 'discovery-generation':
        await sql`UPDATE discovered_provider_accounts SET discovery_generation=2 WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
        break
      case 'inactive':
        await sql`UPDATE provider_connections SET state='discovered' WHERE deployment_id=${deploymentId} AND connection_id=${connectionId}::uuid`
    }
  }
  for (const phase of ['claim', 'before-delivery', 'finalization'] as const) {
    it.each(authorityChanges)(`fences real %s at ${phase}`, async kind => {
      const op = lifecycle(`${phase}-${kind}`)
      const seeded = await seed(op)
      // Exercise migration 0009's real trigger as well as the selected dead letter.
      await sql`INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${subject},${seeded.connectionId + ':provider-account'},1,${op.idempotencyKey + '-witness'},${sql.json(op as never)},'uncertain')`
      let resolves = 0, deliveries = 0
      const mutate = () => changeAuthority(kind, seeded.connectionId)
      const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
        resolve: async () => { resolves++; if (phase === 'before-delivery') await mutate(); return { lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinaryOperation(op), boundReceipt: receipt(priorLifecycle(op)) } },
        deliver: async candidate => { deliveries++; if (phase === 'finalization') await mutate(); return receipt(candidate) },
      })
      if (phase === 'claim') await mutate()
      await expect(control.recover(subject, requestFor(seeded))).rejects.toThrow(/authority/)
      expect(resolves).toBe(phase === 'claim' ? 0 : 1)
      expect(deliveries).toBe(phase === 'finalization' ? 1 : 0)
      expect((await sql`SELECT state,provider_generation,last_error FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0]).toEqual({ state: 'dead_letter', provider_generation: 1, last_error: 'original bridge error' })
      expect((await sql`SELECT count(*)::int AS count FROM investec_terminal_revision_recovery_audit WHERE outbox_id=${seeded.id}::uuid`)[0].count).toBe(0)
      if (kind === 'rotation') expect((await sql`SELECT state,provider_generation,dead_letter_reason FROM projection_outbox WHERE deployment_id=${deploymentId} AND idempotency_key=${op.idempotencyKey + '-witness'}`)[0]).toEqual({ state: 'dead_letter', provider_generation: 1, dead_letter_reason: 'PROVIDER_GENERATION_SUPERSEDED' })
    })
  }

  it('rejects exact payload mutation during delivery and rolls back suppression and audit', async () => {
    const op = lifecycle('final-payload-cas'), seeded = await seed(op)
    const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinaryOperation(op), boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async candidate => {
        await sql`UPDATE projection_outbox SET payload=jsonb_set(payload,'{activity,amount}','"2"') WHERE id=${seeded.id}::uuid`
        return receipt(candidate)
      },
    })
    await expect(control.recover(subject, requestFor(seeded))).rejects.toThrow(/CAS/)
    expect((await sql`SELECT state,payload->'activity'->>'amount' AS amount FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0]).toEqual({ state: 'dead_letter', amount: '2' })
    expect((await sql`SELECT count(*)::int AS count FROM investec_terminal_revision_recovery_audit WHERE outbox_id=${seeded.id}::uuid`)[0].count).toBe(0)
  })

  it('reclaims an expired crashed delivery with exact saved evidence and fences the stale owner', async () => {
    const op = lifecycle('expired-crash'), seeded = await seed(op)
    const oldPost = deferred(), postEntered = deferred(), newGet = deferred(), getEntered = deferred()
    const calls: string[] = []
    let committed = false
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const target = String(input)
      calls.push(`${init?.method ?? 'GET'} ${target}`)
      if (target.endsWith('/' + encodeURIComponent(op.idempotencyKey))) return new Response(null, { status: 404 })
      if (target.endsWith('/' + encodeURIComponent(seeded.l1.idempotencyKey))) return new Response(JSON.stringify(receipt(seeded.l1)))
      if (init?.method === 'POST') {
        committed = true
        postEntered.release()
        await oldPost.promise
        const wire = JSON.parse(String(init.body))
        return new Response(JSON.stringify(receipt({ ...ordinaryOperation(op), idempotencyKey: wire.idempotency_key })))
      }
      if (!committed) return new Response(null, { status: 404 })
      getEntered.release()
      await newGet.promise
      return new Response(JSON.stringify(receipt({ ...ordinaryOperation(op), idempotencyKey: decodeURIComponent(target.split('/').pop()!) })))
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(runtimeSql, deploymentId, {
        FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
        FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
      })
      const resolver = deps.terminalRevisionResolver!
      const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, resolver)
      const first = control.recover(subject, requestFor(seeded)).then(value => ({ value, error: '' }), error => ({ value: null, error: String(error) }))
      await postEntered.promise
      const [saved] = await sql`SELECT ordinary_operation,bound_lifecycle_operation,bound_lifecycle_receipt,claim_owner FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${seeded.id}::uuid`
      await sql`UPDATE investec_terminal_revision_recovery_intents SET claim_lease_until=clock_timestamp()-interval '1 second' WHERE outbox_id=${seeded.id}::uuid`
      const retryControl = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
        ...resolver, resolve: async () => { throw new Error('must not regenerate saved operation or L1 evidence') },
      })
      const retry = retryControl.recover(subject, requestFor(seeded))
      await getEntered.promise
      const [reclaimed] = await sql`SELECT ordinary_operation,bound_lifecycle_operation,bound_lifecycle_receipt,claim_owner FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${seeded.id}::uuid`
      expect(reclaimed.ordinary_operation).toEqual(saved.ordinary_operation)
      expect(reclaimed.bound_lifecycle_receipt).toEqual(saved.bound_lifecycle_receipt)
      expect(reclaimed.bound_lifecycle_operation).toEqual(seeded.l1)
      expect(saved.bound_lifecycle_operation).toEqual(seeded.l1)
      expect(reclaimed.claim_owner).not.toBe(saved.claim_owner)
      oldPost.release()
      expect((await first).error).toMatch(/lease lost/)
      expect((await sql`SELECT state,claim_owner FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${seeded.id}::uuid`)[0]).toEqual({ state: 'claimed', claim_owner: reclaimed.claim_owner })
      expect((await sql`SELECT state FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0].state).toBe('dead_letter')
      newGet.release()
      expect((await retry).state).toBe('suppressed')
      expect(calls.map(call => call.split(' ')[0])).toEqual(['GET', 'GET', 'POST', 'GET'])
      expect(calls.filter(call => call.endsWith('/' + encodeURIComponent(op.idempotencyKey)))).toHaveLength(0)
      expect(calls.filter(call => call.endsWith('/' + encodeURIComponent(seeded.l1.idempotencyKey)))).toHaveLength(1)
      expect((await sql`SELECT count(*)::int AS count FROM investec_terminal_revision_recovery_audit WHERE outbox_id=${seeded.id}::uuid`)[0].count).toBe(1)
    } finally { oldPost.release(); newGet.release(); globalThis.fetch = originalFetch }
  })

  it('rechecks authority between ordinary receipt GET and POST in the runtime resolver', async () => {
    const op = lifecycle('get-post-rotation'), seeded = await seed(op)
    const calls: string[] = []
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      calls.push(init?.method ?? 'GET')
      if (String(input).endsWith('/' + encodeURIComponent(seeded.l1.idempotencyKey))) return new Response(JSON.stringify(receipt(seeded.l1)))
      await changeAuthority('rotation', seeded.connectionId)
      return new Response(null, { status: 404 })
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(runtimeSql, deploymentId, {
        FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
        FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
      })
      const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, deps.terminalRevisionResolver!)
      await expect(control.recover(subject, requestFor(seeded))).rejects.toThrow(/authority/)
      expect(calls).toEqual(['GET', 'GET'])
    } finally { globalThis.fetch = originalFetch }
  })

  it.each(['account', 'resource', 'nonposted'] as const)('rejects resolver %s scope changes before delivery', async scope => {
    const op = lifecycle(`scope-${scope}`), seeded = await seed(op)
    const ordinary = ordinaryOperation(op)
    const changed = scope === 'account' ? { ...ordinary, accountId: 'other-account' }
      : scope === 'resource' ? { ...ordinary, resourceId: 'other-resource' }
      : { ...ordinary, activity: { ...ordinary.activity!, status: 'PENDING' as const } }
    let deliveries = 0
    const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: changed, boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async candidate => { deliveries++; return receipt(candidate) },
    })
    await expect(control.recover(subject, requestFor(seeded))).rejects.toThrow(/scope mismatch/)
    expect(deliveries).toBe(0)
    expect((await sql`SELECT state FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0].state).toBe('dead_letter')
  })

  it.each(['lifecycle', 'ordinary'] as const)('rejects unbound %s receipts without suppression or audit', async phase => {
    const op = lifecycle(`receipt-${phase}`), seeded = await seed(op)
    let deliveries = 0
    const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinaryOperation(op),
        boundReceipt: receipt(phase === 'lifecycle' ? { ...priorLifecycle(op), idempotencyKey: 'other-lifecycle' } : priorLifecycle(op)) }),
      deliver: async candidate => { deliveries++; return receipt({ ...candidate, idempotencyKey: 'other-ordinary' }) },
    })
    await expect(control.recover(subject, requestFor(seeded))).rejects.toThrow()
    expect(deliveries).toBe(phase === 'lifecycle' ? 0 : 1)
    expect((await sql`SELECT state FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0].state).toBe('dead_letter')
    expect((await sql`SELECT count(*)::int AS count FROM investec_terminal_revision_recovery_audit WHERE outbox_id=${seeded.id}::uuid`)[0].count).toBe(0)
  })

  it('recovers legacy L2 without a revision from latest exact canonical facts, never elevating L2 metadata', async () => {
    const { observationRevision: _revision, ...legacy } = lifecycle('legacy-no-revision')
    const seeded = await seed(legacy)
    const { observationRevision: _l1Revision, ...l1 } = seeded.l1
    await sql`UPDATE projection_outbox SET payload=${sql.json(l1 as never)} WHERE deployment_id=${deploymentId} AND idempotency_key=${l1.idempotencyKey}`
    const calls: string[] = []
    let wire: Record<string, any> | undefined
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const target = String(input)
      calls.push(`${init?.method ?? 'GET'} ${target}`)
      if (target.endsWith('/' + encodeURIComponent(legacy.idempotencyKey))) return new Response(null, { status: 404 })
      if (target.endsWith('/' + encodeURIComponent(l1.idempotencyKey))) return new Response(JSON.stringify(receipt(l1)))
      if (init?.method === 'POST') {
        wire = JSON.parse(String(init.body))
        return new Response(JSON.stringify(receipt({ ...ordinaryOperation(legacy), idempotencyKey: wire!.idempotency_key })))
      }
      return new Response(null, { status: 404 })
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(runtimeSql, deploymentId, {
        FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
        FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
      })
      const result = await createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, deps.terminalRevisionResolver!).recover(subject, requestFor(seeded))
      expect(result.state).toBe('suppressed')
      const [latest] = await sql`SELECT observation_revision FROM observation_revision_ledger WHERE deployment_id=${deploymentId} AND source_resource_incarnation_id=${seeded.source}::uuid AND semantic_hash=${'c'.repeat(64)}`
      expect(wire?.observation_revision).toBe(Number(latest.observation_revision))
      expect(wire?.activity.metadata.providerOwned.investec.actionDate).toBe('2026-09-16')
      expect(legacy.activity?.metadata).toMatchObject({ providerOwned: { investec: { actionDate: '2026-09-15' } } })
      expect(calls.map(call => call.split(' ')[0])).toEqual(['GET', 'GET', 'POST'])
      expect(calls[0]).toContain(encodeURIComponent(l1.idempotencyKey))
      expect(calls.some(call => call.endsWith('/' + encodeURIComponent(legacy.idempotencyKey)))).toBe(false)
      const [intent] = await sql`SELECT bound_lifecycle_operation,ordinary_operation FROM investec_terminal_revision_recovery_intents WHERE outbox_id=${seeded.id}::uuid`
      expect(intent.bound_lifecycle_operation).toEqual(l1)
      expect(intent.ordinary_operation.observationRevision).toBe(Number(latest.observation_revision))
    } finally { globalThis.fetch = originalFetch }
  })

  it.each(['subject', 'account', 'predecessor', 'successor', 'deployment', 'generation'] as const)('rejects retained L1 from another %s before GET', async field => {
    const seeded = await seed(lifecycle(`wrong-l1-${field}`))
    const l1 = seeded.l1
    const payload = field === 'subject' ? { ...l1, subject: 'urn:wealthfolio:test#other' }
      : field === 'account' ? { ...l1, accountId: 'other' }
      : field === 'predecessor' ? { ...l1, resourceId: `${deploymentId}:${randomUUID()}:other` }
      : field === 'successor' ? { ...l1, lifecycleSuccessorId: `${deploymentId}:${randomUUID()}:other` }
      : field === 'deployment' ? { ...l1, deploymentId: 'other' } : l1
    await sql`UPDATE projection_outbox SET payload=${sql.json(payload as never)},provider_generation=${field === 'generation' ? 2 : 1} WHERE deployment_id=${deploymentId} AND idempotency_key=${l1.idempotencyKey}`
    let fetches = 0
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => { fetches++; throw new Error('unexpected network') }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(runtimeSql, deploymentId, {
        FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge', FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
        FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test', FINANCE_SYNC_BRIDGE_TOKEN: 'test',
      })
      await expect(createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, deps.terminalRevisionResolver!).recover(subject, requestFor(seeded))).rejects.toThrow(/scope/)
      expect(fetches).toBe(0)
      expect((await sql`SELECT state FROM projection_outbox WHERE id=${seeded.id}::uuid`)[0].state).toBe('dead_letter')
    } finally { globalThis.fetch = originalFetch }
  })

  it('enforces runtime mutable intents, append-only audit/ledger and restrictive outbox foreign keys', async () => {
    const op = lifecycle('privileges'), seeded = await seed(op)
    const control = createInvestecTerminalRevisionRecoveryControl(runtimeSql, deploymentId, {
      resolve: async () => ({ lifecycleOperation: priorLifecycle(op), ordinaryOperation: ordinaryOperation(op), boundReceipt: receipt(priorLifecycle(op)) }),
      deliver: async candidate => receipt(candidate),
    })
    const applied = await control.recover(subject, requestFor(seeded))
    expect(applied.state).toBe('suppressed')
    expect((await runtimeSql`SELECT pg_has_role(current_user,relowner,'MEMBER') AS owns_table FROM pg_class WHERE oid='investec_terminal_revision_recovery_intents'::regclass`)[0].owns_table).toBe(false)
    await expect(runtimeSql`UPDATE raw_observations SET provider_version='tampered' WHERE false`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`DELETE FROM raw_observations WHERE false`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`UPDATE investec_terminal_revision_recovery_audit SET reason='tampered' WHERE intent_id=${applied.intentId}::uuid`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`DELETE FROM investec_terminal_revision_recovery_audit WHERE intent_id=${applied.intentId}::uuid`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`DELETE FROM investec_terminal_revision_recovery_intents WHERE intent_id=${applied.intentId}::uuid`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`UPDATE observation_revision_ledger SET provider_version='tampered' WHERE false`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`DELETE FROM observation_revision_ledger WHERE false`).rejects.toThrow(/permission denied/)
    const [revision] = await runtimeSql`INSERT INTO observation_revision_ledger
      (deployment_id,local_owner_id,source_resource_incarnation_id,provider_version,semantic_hash,hash_specification_version,observation_revision)
      VALUES (${deploymentId},${subject},${randomUUID()},'runtime-ledger-test',${'a'.repeat(64)},1,nextval('finance_sync_observation_revision_seq')) RETURNING observation_revision`
    expect(revision.observation_revision).toBeTruthy()
    await expect(sql`UPDATE observation_revision_ledger SET provider_version='tampered' WHERE observation_revision=${revision.observation_revision}`).rejects.toThrow(/append-only/)
    await expect(sql`DELETE FROM observation_revision_ledger WHERE observation_revision=${revision.observation_revision}`).rejects.toThrow(/append-only/)
    await expect(runtimeSql`UPDATE projected_activity_revision_ledger SET payload_digest=NULL WHERE false`).rejects.toThrow(/permission denied/)
    await expect(runtimeSql`DELETE FROM projected_activity_revision_ledger WHERE false`).rejects.toThrow(/permission denied/)
    const successor = op.lifecycleSuccessorId!
    expect(await sql`SELECT 1 FROM projected_activity_revision_ledger WHERE deployment_id=${deploymentId} AND resource_id=${successor}`).toHaveLength(1)
    await expect(sql`UPDATE projected_activity_revision_ledger SET payload_digest=NULL WHERE deployment_id=${deploymentId} AND resource_id=${successor}`).rejects.toThrow(/append-only/)
    await expect(sql`DELETE FROM projected_activity_revision_ledger WHERE deployment_id=${deploymentId} AND resource_id=${successor}`).rejects.toThrow(/append-only/)
    await expect(sql`UPDATE investec_terminal_revision_recovery_audit SET reason='tampered' WHERE intent_id=${applied.intentId}::uuid`).rejects.toThrow(/append-only/)
    await expect(sql`DELETE FROM investec_terminal_revision_recovery_audit WHERE intent_id=${applied.intentId}::uuid`).rejects.toThrow(/append-only/)
    await expect(sql`DELETE FROM projection_outbox WHERE id=${seeded.id}::uuid`).rejects.toThrow(/foreign key/)
    const constraints = await sql`SELECT conrelid::regclass::text AS table_name,confdeltype FROM pg_constraint
      WHERE conrelid IN ('investec_terminal_revision_recovery_intents'::regclass,'investec_terminal_revision_recovery_audit'::regclass)
        AND confrelid='projection_outbox'::regclass AND contype='f'`
    expect(constraints).toHaveLength(2)
    expect(constraints.every(row => row.confdeltype === 'r')).toBe(true)
  })
})
