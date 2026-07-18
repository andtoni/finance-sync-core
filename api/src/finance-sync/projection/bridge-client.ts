
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
  /** Local receipt-binding context; omitted from the HTTP request body. */
  readonly deploymentId: string
  readonly accountId: string
  readonly idempotencyKey: string
  readonly subject: string
  readonly accessGeneration: number
  readonly kind: BridgeDisclosureKind
  readonly resourceId: string
  readonly activity?: BridgeActivityPayload
  readonly lifecycleSuccessorId?: string
}

export interface BridgeReceipt {
  readonly deploymentId: string
  readonly accountId: string
  readonly subject: string
  readonly idempotencyKey: string
  readonly accessGeneration: number
  readonly operation: BridgeWireOperation
  readonly resourceId: string
  readonly lifecycleSuccessorId: string | null
  readonly outcome: 'Accepted' | 'Revoked'
  readonly bindingVersion: 1
  readonly receiptBinding: string
}

export interface BridgeTransport {
  commit(op: BridgeDisclosureOperation): Promise<unknown>
}

export function assertIssuerQualifiedSubject(value: string): string {
  const subject = value.trim()
  const separator = subject.indexOf('#')
  if (separator <= 0 || separator === subject.length - 1) throw new Error('issuer-qualified subject required')
  return subject
}

function receiptBinding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v1;${fields.map(field => `${encoder.encode(field).byteLength}:${field}`).join('')}`
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
  const successor = operation.lifecycleSuccessorId ?? null
  if (receipt.deploymentId !== operation.deploymentId || receipt.accountId !== operation.accountId || receipt.lifecycleSuccessorId !== successor || receipt.bindingVersion !== 1) {
    throw new Error('Bridge receipt binding identity mismatch')
  }
  const expected = receiptBinding([
    operation.deploymentId,
    operation.subject,
    operation.idempotencyKey,
    String(operation.accessGeneration),
    WIRE_OPERATION[operation.kind],
    operation.resourceId,
    operation.accountId,
    successor ?? '<null>',
    'accepted',
  ])
  if (receipt.receiptBinding !== expected) throw new Error('Bridge receipt binding mismatch')
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

export function idempotencyKey(
  deploymentId: string,
  semanticHash: string,
  scope: readonly string[] = [],
): string {
  const encoder = new TextEncoder()
  const fields = [deploymentId, ...scope, semanticHash]
  const framed = fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')
  return createHash('sha256').update(framed, 'utf8').digest('hex').slice(0, 40)
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
  connectionId = observation.sourceResourceIncarnationId,
): BridgeDisclosureOperation {
  const p = observation.payload as Record<string, unknown>
  const subject = assertIssuerQualifiedSubject(observation.localOwnerId)
  const transactionId = String(p.transactionId ?? p.transaction_id ?? p.sourceId ?? observation.semanticHash)
  const resourceId = `${observation.deploymentId}:${observation.sourceResourceIncarnationId}:${transactionId}`
  const pendingTransactionId = typeof p.pendingTransactionId === 'string' && p.pendingTransactionId.length > 0
    ? p.pendingTransactionId
    : undefined
  const pendingResourceId = pendingTransactionId === undefined
    ? undefined
    : `${observation.deploymentId}:${observation.sourceResourceIncarnationId}:${pendingTransactionId}`
  const common = {
    deploymentId: observation.deploymentId,
    accountId,
    idempotencyKey: idempotencyKey(observation.deploymentId, observation.semanticHash, [
      observation.localOwnerId,
      connectionId,
      observation.sourceResourceIncarnationId,
      accountId,
      resourceId,
    ]),
    subject,
    accessGeneration,
  }
  if (p.isRemoved === true) {
    return { ...common, kind: 'ActivityDelete', resourceId }
  }
  const plaidAmount = Number(p.amount)
  if (!Number.isFinite(plaidAmount) || plaidAmount === 0) throw new Error('invalid provider amount')
  if (typeof p.currency !== 'string' || p.currency.length === 0) throw new Error('invalid provider currency')
  return {
    ...common,
    kind: 'ActivityUpsert',
    resourceId: pendingResourceId ?? resourceId,
    ...(pendingResourceId === undefined ? {} : { lifecycleSuccessorId: resourceId }),
    activity: {
      accountId,
      // Plaid uses positive for money leaving the account and negative for money entering it.
      activityType: plaidAmount > 0 ? 'WITHDRAWAL' : 'DEPOSIT',
      status: p.pending === true ? 'PENDING' : 'POSTED',
      activityDate: (p.date as string) ?? observation.observedAt.slice(0, 10),
      amount: String(Math.abs(plaidAmount)),
      currency: p.currency,
    },
  }
}

export function providerObservationToBridge(
  provider: 'investec' | 'mercury' | 'plaid' | 'snaptrade',
  observation: {
    readonly deploymentId: string
    readonly localOwnerId: string
    readonly sourceResourceIncarnationId: string
    readonly semanticHash: string
    readonly domain: string
    readonly payload: Readonly<Record<string, unknown>>
    readonly observedAt: string
  },
  accessGeneration: number,
  accountId: string,
  connectionId = observation.sourceResourceIncarnationId,
): BridgeDisclosureOperation | null {
  const payload = observation.payload
  const hasTransactionIdentity = typeof payload.transactionId === 'string'
    || typeof payload.transaction_id === 'string'
    || typeof payload.sourceId === 'string'
  if (!hasTransactionIdentity) return null // balances, holdings and securities remain canonical facts only
  if (payload.isRemoved === true) return cashObservationToBridge(observation, accessGeneration, accountId, connectionId)

  const amount = Number(payload.amount)
  if (!Number.isFinite(amount) || amount === 0) throw new Error('invalid provider amount')
  if (provider === 'plaid') return cashObservationToBridge(observation, accessGeneration, accountId, connectionId)
  if (provider === 'investec' || provider === 'mercury') {
    // Direct-bank adapters use ledger signs (negative money-out); Plaid uses the inverse.
    return cashObservationToBridge({ ...observation, payload: { ...payload, amount: -amount } }, accessGeneration, accountId, connectionId)
  }
  if (observation.domain !== 'brokerage_activity') return null
  const activityTypes: Readonly<Record<string, string>> = {
    buy: 'BUY', sell: 'SELL', dividend: 'DIVIDEND', contribution: 'DEPOSIT',
    withdrawal: 'WITHDRAWAL', transfer: 'TRANSFER', fee: 'FEE', interest: 'INTEREST',
  }
  const activityType = activityTypes[String(payload.type)]
  if (!activityType) throw new Error('unsupported brokerage activity type')
  const base = cashObservationToBridge({ ...observation, payload: { ...payload, amount: -Math.abs(amount) } }, accessGeneration, accountId, connectionId)
  if (!base.activity) throw new Error('brokerage activity projection failed')
  return { ...base, activity: { ...base.activity, activityType, amount: String(Math.abs(amount)), status: 'POSTED' } }
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
