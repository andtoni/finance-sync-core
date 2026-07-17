
import { createHash } from 'node:crypto'

export type BridgeDisclosureKind = 'ActivityUpsert' | 'ActivityDelete' | 'ProjectionFence'
export type BridgeWireOperation = 'activity_upsert' | 'activity_delete' | 'projection_fence'

const WIRE_OPERATION: Record<BridgeDisclosureKind, BridgeWireOperation> = {
  ActivityUpsert: 'activity_upsert',
  ActivityDelete: 'activity_delete',
  ProjectionFence: 'projection_fence',
}

const REQUEST_KIND: Record<BridgeDisclosureKind, BridgeDisclosureKind> = {
  ActivityUpsert: 'ActivityUpsert',
  ActivityDelete: 'ActivityDelete',
  ProjectionFence: 'ProjectionFence',
}

export interface BridgeActivityPayload {
  readonly accountId: string
  readonly activityType: string
  readonly status: string
  readonly activityDate: string
  readonly amount: string
  readonly currency: string
  readonly notes?: string
  readonly activityTypeOverride?: string
  readonly needsReview?: boolean
}

export interface BridgeDisclosureOperation {
  readonly idempotencyKey: string
  readonly subject: string
  readonly accessGeneration: number
  readonly kind: BridgeDisclosureKind
  readonly resourceId: string
  readonly activity?: BridgeActivityPayload
  readonly lifecycleSuccessorId?: string
}

export interface BridgeReceipt {
  readonly subject: string
  readonly idempotencyKey: string
  readonly accessGeneration: number
  readonly operation: BridgeWireOperation
  readonly resourceId: string
  readonly outcome: 'Accepted' | 'Revoked'
}

export interface BridgeTransport {
  commit(op: BridgeDisclosureOperation): Promise<BridgeReceipt>
}

export function assertIssuerQualifiedSubject(value: string): string {
  const subject = value.trim()
  const separator = subject.indexOf('#')
  if (separator <= 0 || separator === subject.length - 1) throw new Error('issuer-qualified subject required')
  return subject
}

export function validateBridgeReceipt(operation: BridgeDisclosureOperation, value: unknown): BridgeReceipt {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid bridge receipt')
  const receipt = value as Record<string, unknown>
  if (receipt.outcome !== 'Accepted') throw new Error('bridge receipt rejected')
  if (receipt.operation !== WIRE_OPERATION[operation.kind]) throw new Error('Bridge operation mismatch')
  if (receipt.subject !== operation.subject) throw new Error('Bridge subject mismatch')
  if (receipt.idempotencyKey !== operation.idempotencyKey || receipt.resourceId !== operation.resourceId || receipt.accessGeneration !== operation.accessGeneration) {
    throw new Error('Bridge receipt mismatch')
  }
  return receipt as unknown as BridgeReceipt
}

export function bridgeOperationToWire(operation: BridgeDisclosureOperation): Record<string, unknown> {
  const activity = operation.activity && {
    account_id: operation.activity.accountId,
    activity_type: operation.activity.activityType,
    status: operation.activity.status,
    activity_date: operation.activity.activityDate,
    amount: operation.activity.amount,
    currency: operation.activity.currency,
    ...(operation.activity.notes === undefined ? {} : { notes: operation.activity.notes }),
    ...(operation.activity.activityTypeOverride === undefined ? {} : { activity_type_override: operation.activity.activityTypeOverride }),
    ...(operation.activity.needsReview === undefined ? {} : { needs_review: operation.activity.needsReview }),
  }
  return {
    idempotency_key: operation.idempotencyKey,
    subject: operation.subject,
    access_generation: operation.accessGeneration,
    kind: REQUEST_KIND[operation.kind],
    resource_id: operation.resourceId,
    ...(activity === undefined ? {} : { activity }),
    ...(operation.lifecycleSuccessorId === undefined ? {} : { lifecycle_successor_id: operation.lifecycleSuccessorId }),
  }
}

export type ObservationToBridge = (observation: {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly semanticHash: string
  readonly domain: string
  readonly payload: Readonly<Record<string, unknown>>
  readonly observedAt: string
}) => BridgeDisclosureOperation

export function idempotencyKey(deploymentId: string, semanticHash: string): string {
  return createHash('sha256').update(`${deploymentId}:${semanticHash}`, 'utf8').digest('hex').slice(0, 40)
}

export function cashObservationToBridge(
  observation: {
    readonly deploymentId: string
    readonly localOwnerId: string
    readonly sourceResourceIncarnationId: string
    readonly semanticHash: string
    readonly payload: Readonly<Record<string, unknown>>
    readonly observedAt: string
  },
  accessGeneration: number,
  accountId: string,
): BridgeDisclosureOperation {
  const p = observation.payload as Record<string, unknown>
  const subject = assertIssuerQualifiedSubject(observation.localOwnerId)
  return {
    idempotencyKey: idempotencyKey(observation.deploymentId, observation.semanticHash),
    subject,
    accessGeneration,
    kind: 'ActivityUpsert',
    resourceId: `${observation.deploymentId}:${observation.sourceResourceIncarnationId}:${p.transactionId ?? p.transaction_id ?? observation.semanticHash}`,
    activity: {
      accountId,
      activityType: (p.isRemoved as boolean) ? 'REMOVAL' : String(p.amount ?? '0').startsWith('-') ? 'WITHDRAWAL' : 'DEPOSIT',
      status: (p.isRemoved as boolean) ? 'VOID' : 'CLEARED',
      activityDate: (p.date as string) ?? observation.observedAt.slice(0, 10),
      amount: String(p.amount ?? '0').replace(/^-/, ''),
      currency: (p.currency as string) ?? 'USD',
      notes: [p.name as string, p.description as string].filter(Boolean).join(' | ') || undefined,
      activityTypeOverride: (p.transactionType as string) === 'CREDIT' ? 'DEPOSIT' : (p.transactionType as string) === 'DEBIT' ? 'WITHDRAWAL' : undefined,
      needsReview: (p.pending as boolean) ?? false,
    },
  }
}

export async function commitObservation(
  transport: BridgeTransport,
  operation: BridgeDisclosureOperation,
  maxRetries = 3,
): Promise<BridgeReceipt> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const receipt = await transport.commit(operation)
      return validateBridgeReceipt(operation, receipt)
    } catch (error) {
      if (attempt === maxRetries) throw error
      await new Promise(r => setTimeout(r, 1000 * attempt))
    }
  }
  throw new Error('exhausted retries')
}

export async function commitObservationBatch(
  transport: BridgeTransport,
  observations: readonly BridgeDisclosureOperation[],
): Promise<BridgeReceipt[]> {
  const receipts: BridgeReceipt[] = []
  for (const op of observations) {
    receipts.push(await commitObservation(transport, op))
  }
  return receipts
}
