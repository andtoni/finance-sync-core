import { createHash, randomUUID } from 'node:crypto'
import type { BridgeDisclosureOperation, BridgeReceipt } from './projection/bridge-client'
import { validateBridgeReceipt } from './projection/bridge-client'
import type { FinanceSyncSql } from './storage/pg'

export interface TerminalRevisionRecoveryRequest {
  readonly outboxId: string
  readonly expectedPayloadHash: string
  readonly reason: string
  readonly apply?: boolean
}
export interface TerminalRevisionRecoveryResult {
  readonly intentId: string
  readonly state: 'planned' | 'claimed' | 'delivered' | 'suppressed'
  readonly idempotent: boolean
  readonly operation?: BridgeDisclosureOperation
  readonly receipt?: BridgeReceipt
}
export interface TerminalRevisionRecoveryResolver {
  resolve(operation: BridgeDisclosureOperation): Promise<{
    readonly lifecycleOperation: BridgeDisclosureOperation
    readonly ordinaryOperation: BridgeDisclosureOperation
    readonly boundReceipt: unknown
  }>
  deliver(operation: BridgeDisclosureOperation, beforePost?: () => Promise<void>): Promise<unknown>
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical)
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => [key, canonical(item)]))
  return value
}
export function terminalRevisionPayloadHash(operation: BridgeDisclosureOperation): string {
  return createHash('sha256').update(JSON.stringify(canonical(operation)), 'utf8').digest('hex')
}
function validHash(value: string): boolean { return /^[a-f0-9]{64}$/i.test(value) }
function validReason(value: string): boolean { return value.trim().length > 0 && value.trim().length <= 2048 }

type SelectedRow = { connection_id: string; provider_generation: number; payload: BridgeDisclosureOperation; state: string; last_error: string | null }

/** Durable, one-row-at-a-time operator recovery. Network I/O is never inside a SQL transaction. */
export function createInvestecTerminalRevisionRecoveryControl(
  sql: FinanceSyncSql,
  deploymentId: string,
  resolver: TerminalRevisionRecoveryResolver,
) {
  return {
    async recover(subject: string, request: TerminalRevisionRecoveryRequest): Promise<TerminalRevisionRecoveryResult> {
      if (!subject || !validHash(request.expectedPayloadHash) || !validReason(request.reason)) throw new Error('invalid terminal revision recovery request')
      const expectedHash = request.expectedPayloadHash.toLowerCase()
      const claimOwner = randomUUID()
      const selectRow = async (tx: FinanceSyncSql, lock = false): Promise<SelectedRow> => {
        const rows = await tx<SelectedRow[]>`
          SELECT connection_id, provider_generation, payload, state, last_error FROM projection_outbox
          WHERE deployment_id=${deploymentId} AND local_owner_id=${subject} AND id=${request.outboxId}::uuid
            AND payload->>'kind'='ActivityUpsert' AND payload->>'lifecycleSuccessorId' IS NOT NULL
          ${lock ? tx`FOR UPDATE` : tx``}
        `
        const row = rows[0]
        if (!row || terminalRevisionPayloadHash(row.payload) !== expectedHash) throw new Error('payload CAS mismatch')
        return row
      }
      const validateResolution = async (row: SelectedRow, resolved: Awaited<ReturnType<TerminalRevisionRecoveryResolver['resolve']>>) => {
        const l1 = resolved.lifecycleOperation
        const ordinary = resolved.ordinaryOperation
        if (l1.idempotencyKey === row.payload.idempotencyKey || l1.kind !== 'ActivityUpsert'
          || l1.deploymentId !== deploymentId || l1.subject !== subject || l1.accountId !== row.payload.accountId
          || l1.resourceId !== row.payload.resourceId || l1.lifecycleSuccessorId !== row.payload.lifecycleSuccessorId
          || ordinary.kind !== 'ActivityUpsert' || ordinary.deploymentId !== deploymentId || ordinary.subject !== subject
          || ordinary.accountId !== row.payload.accountId || ordinary.accessGeneration !== row.payload.accessGeneration
          || ordinary.resourceId !== row.payload.lifecycleSuccessorId || ordinary.lifecycleSuccessorId !== undefined
          || ordinary.activity?.accountId !== row.payload.accountId || ordinary.activity.status !== 'POSTED'
          || !ordinary.idempotencyKey || ordinary.idempotencyKey === row.payload.idempotencyKey
          || ordinary.idempotencyKey === l1.idempotencyKey || (ordinary.supersededResourceIds?.length ?? 0) !== 0) throw new Error('recovery operation scope mismatch')
        const retained = await sql`
          SELECT l1.id FROM projection_outbox l1 JOIN projection_outbox selected ON selected.id=${request.outboxId}::uuid
          WHERE l1.deployment_id=${deploymentId} AND l1.local_owner_id=${subject} AND l1.connection_id=${row.connection_id}
            AND l1.provider_generation=${row.provider_generation} AND l1.state='delivered' AND l1.created_at < selected.created_at
            AND l1.idempotency_key=${l1.idempotencyKey} AND l1.payload=${sql.json(l1 as never)}
        `
        if (retained.length !== 1) throw new Error('retained delivered lifecycle evidence is outside recovery scope')
        return validateBridgeReceipt(l1, resolved.boundReceipt)
      }
      // Dry-run is deliberately read-only: no intent, audit, claim, or lock is written.
      if (request.apply !== true) {
        const row = await selectRow(sql)
        const resolved = await resolver.resolve(row.payload)
        const receipt = await validateResolution(row, resolved)
        return { intentId: '', state: 'planned', idempotent: false, operation: resolved.ordinaryOperation, receipt }
      }
      const assertAuthority = async (tx: FinanceSyncSql, row: SelectedRow) => {
        // Lock authority before the outbox, matching generation rotation's lock order.
        // Hold these locks through finalization: rotation leaves old rows dead-lettered,
        // so an unchanged outbox generation is not sufficient authorization.
        const authorized = await tx`
          SELECT 1 FROM provider_connections pc
          JOIN discovered_provider_accounts dpa
            ON dpa.deployment_id=pc.deployment_id AND dpa.local_owner_id=pc.local_owner_id AND dpa.connection_id=pc.connection_id
          JOIN provider_account_admissions paa
            ON paa.deployment_id=dpa.deployment_id AND paa.local_owner_id=dpa.local_owner_id
            AND paa.connection_id=dpa.connection_id AND paa.provider_account_id=dpa.provider_account_id
          WHERE pc.deployment_id=${deploymentId} AND pc.local_owner_id=${subject} AND pc.provider='investec'
            AND pc.connection_id::text || ':' || dpa.provider_account_id=${row.connection_id}
            AND pc.state='active' AND pc.disabled_at IS NULL AND pc.provider_generation=${row.provider_generation}
            AND dpa.discovery_generation=pc.provider_generation AND paa.provider_generation=pc.provider_generation
            AND paa.revoked_at IS NULL AND paa.baseline_state='verified'
            AND paa.wealthfolio_account_id=${row.payload.accountId}
            AND paa.receiver_access_generation=${row.payload.accessGeneration}
            AND ${row.payload.deploymentId}=pc.deployment_id AND ${row.payload.subject}=pc.local_owner_id
            AND starts_with(${row.payload.resourceId},pc.deployment_id || ':' || dpa.source_resource_incarnation_id::text || ':')
            AND starts_with(${row.payload.lifecycleSuccessorId!},pc.deployment_id || ':' || dpa.source_resource_incarnation_id::text || ':')
          FOR SHARE OF pc, dpa, paa
        `
        if (authorized.length !== 1) throw new Error('terminal recovery authority changed or is stale')
      }
      const claim = await sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        const selected = await selectRow(tx)
        const completed = await tx<{ intent_id: string }[]>`
          SELECT intent_id FROM investec_terminal_revision_recovery_intents
          WHERE deployment_id=${deploymentId} AND local_owner_id=${subject} AND outbox_id=${request.outboxId}::uuid
            AND expected_payload_hash=${expectedHash} AND mode='apply' AND state='suppressed'
        `
        if (selected.state === 'suppressed' && completed[0]) return { completed: completed[0].intent_id }
        if (selected.state !== 'dead_letter') throw new Error('terminal recovery requires a dead_letter row')
        await assertAuthority(tx, selected)
        const row = await selectRow(tx, true)
        if (row.state !== 'dead_letter' || row.provider_generation !== selected.provider_generation || row.connection_id !== selected.connection_id) throw new Error('selected lifecycle row changed during recovery')
        const existing = await tx<{ intent_id: string; ordinary_operation: BridgeDisclosureOperation | null; bound_lifecycle_receipt: unknown; bound_lifecycle_operation: BridgeDisclosureOperation | null; leased: boolean }[]>`
          SELECT intent_id, ordinary_operation, bound_lifecycle_receipt, bound_lifecycle_operation,
            state='claimed' AND claim_lease_until > clock_timestamp() AS leased
          FROM investec_terminal_revision_recovery_intents
          WHERE deployment_id=${deploymentId} AND local_owner_id=${subject} AND outbox_id=${request.outboxId}::uuid
            AND expected_payload_hash=${expectedHash} AND mode='apply'
          FOR UPDATE
        `
        const prior = existing[0]
        if (prior?.leased) throw new Error('terminal recovery is already claimed')
        if (prior) {
          await tx`UPDATE investec_terminal_revision_recovery_intents SET state='claimed', claim_owner=${claimOwner}::uuid, claim_lease_until=clock_timestamp()+interval '2 minutes' WHERE intent_id=${prior.intent_id}::uuid`
          return { intentId: prior.intent_id, operation: prior.ordinary_operation, lifecycleOperation: prior.bound_lifecycle_operation, receipt: prior.bound_lifecycle_receipt, row }
        }
        const inserted = await tx<{ intent_id: string }[]>`
          INSERT INTO investec_terminal_revision_recovery_intents
            (deployment_id,local_owner_id,outbox_id,expected_payload_hash,reason,actor_subject,mode,state,claim_owner,claim_lease_until,original_last_error)
          VALUES (${deploymentId},${subject},${request.outboxId}::uuid,${expectedHash},${request.reason.trim()},${subject},'apply','claimed',${claimOwner}::uuid,clock_timestamp()+interval '2 minutes',${row.last_error})
          RETURNING intent_id
        `
        return { intentId: inserted[0].intent_id, operation: null, lifecycleOperation: null, receipt: null, row }
      })
      if (claim.completed) return { intentId: claim.completed, state: 'suppressed', idempotent: true }
      const intentId = claim.intentId!
      const assertClaim = async (tx: FinanceSyncSql) => {
        await assertAuthority(tx, claim.row!)
        const row = await selectRow(tx, true)
        if (row.state !== 'dead_letter' || row.provider_generation !== claim.row!.provider_generation || row.connection_id !== claim.row!.connection_id) throw new Error('selected lifecycle row changed during recovery')
        const owned = await tx`
          SELECT intent_id FROM investec_terminal_revision_recovery_intents
          WHERE intent_id=${intentId}::uuid AND state='claimed' AND claim_owner=${claimOwner}::uuid
            AND claim_lease_until > clock_timestamp() FOR UPDATE
        `
        if (owned.length !== 1) throw new Error('terminal recovery claim lease lost')
      }
      try {
        // Once saved, retry uses exactly this ordinary operation and L1 evidence,
        // even if the resolver implementation changes after a crash.
        if ((claim.operation || claim.receipt || claim.lifecycleOperation) && !(claim.operation && claim.receipt && claim.lifecycleOperation)) throw new Error('incomplete saved recovery evidence')
        const resolved = claim.operation && claim.receipt && claim.lifecycleOperation
          ? { ordinaryOperation: claim.operation, lifecycleOperation: claim.lifecycleOperation, boundReceipt: claim.receipt }
          : await resolver.resolve(claim.row!.payload)
        const receipt = await validateResolution(claim.row!, resolved)
        const operation = resolved.ordinaryOperation
        await sql.begin(async transaction => {
          const tx = transaction as unknown as FinanceSyncSql
          await assertClaim(tx)
          await tx`UPDATE investec_terminal_revision_recovery_intents
            SET ordinary_operation=${tx.json(operation as never)}, bound_lifecycle_operation=${tx.json(resolved.lifecycleOperation as never)}, bound_lifecycle_receipt=${tx.json(receipt as never)}
            WHERE intent_id=${intentId}::uuid AND state='claimed' AND claim_owner=${claimOwner}::uuid`
        })
        const beforePost = async () => { await sql.begin(async transaction => assertClaim(transaction as unknown as FinanceSyncSql)) }
        const delivered = validateBridgeReceipt(operation, await resolver.deliver(operation, beforePost))
        await sql.begin(async transaction => {
          const tx = transaction as unknown as FinanceSyncSql
          await assertClaim(tx)
          const owned = await tx`UPDATE investec_terminal_revision_recovery_intents
            SET state='suppressed',claim_lease_until=NULL,completed_at=now(),new_operation_idempotency_key=${delivered.idempotencyKey},new_receipt=${tx.json(delivered as never)}
            WHERE intent_id=${intentId}::uuid AND state='claimed' AND claim_owner=${claimOwner}::uuid AND claim_lease_until > clock_timestamp()`
          if (owned.count !== 1) throw new Error('terminal recovery claim lease lost')
          const changed = await tx`
            UPDATE projection_outbox SET state='suppressed', dead_letter_reason='TERMINAL_REVISION_RECONCILED'
            WHERE id=${request.outboxId}::uuid AND deployment_id=${deploymentId} AND local_owner_id=${subject}
              AND connection_id=${claim.row!.connection_id} AND provider_generation=${claim.row!.provider_generation} AND state='dead_letter'
              AND payload=${tx.json(claim.row!.payload as never)}
          `
          if (changed.count !== 1) throw new Error('selected lifecycle row changed during recovery')
          await tx`INSERT INTO investec_terminal_revision_recovery_audit
            (deployment_id,local_owner_id,intent_id,outbox_id,new_operation_idempotency_key,new_receipt,actor_subject,reason)
            SELECT deployment_id,local_owner_id,intent_id,outbox_id,new_operation_idempotency_key,new_receipt,actor_subject,reason
            FROM investec_terminal_revision_recovery_intents WHERE intent_id=${intentId}::uuid AND claim_owner=${claimOwner}::uuid AND state='suppressed'`
        })
        return { intentId, state: 'suppressed', idempotent: false, operation, receipt: delivered }
      } catch (error) {
        await sql`UPDATE investec_terminal_revision_recovery_intents SET state='failed',claim_lease_until=NULL
          WHERE intent_id=${intentId}::uuid AND state='claimed' AND claim_owner=${claimOwner}::uuid AND claim_lease_until > clock_timestamp()`
        throw error
      }
    },
  }
}
