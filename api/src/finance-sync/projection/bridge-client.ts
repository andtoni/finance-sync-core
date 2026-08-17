
import { createHash } from 'node:crypto'
import { currentStateSemanticHash, type CurrentStatePayload } from './current-state'

export type BridgeDisclosureKind = 'ActivityUpsert' | 'ActivityDelete' | 'CurrentStateUpsert' | 'ProjectionFence'
export type BridgeWireOperation = 'activity_upsert' | 'activity_delete' | 'current_state_upsert' | 'projection_fence'

const WIRE_OPERATION: Record<BridgeDisclosureKind, BridgeWireOperation> = {
  ActivityUpsert: 'activity_upsert',
  ActivityDelete: 'activity_delete',
  CurrentStateUpsert: 'current_state_upsert',
  ProjectionFence: 'projection_fence',
}

const REQUEST_KIND: Record<BridgeDisclosureKind, BridgeDisclosureKind> = {
  ActivityUpsert: 'ActivityUpsert',
  ActivityDelete: 'ActivityDelete',
  CurrentStateUpsert: 'CurrentStateUpsert',
  ProjectionFence: 'ProjectionFence',
}

export interface BridgeActivityPayload {
  readonly accountId: string
  readonly activityType: string
  readonly status: string
  readonly activityDate: string
  readonly activityDatePrecision: 'DATE' | 'INSTANT'
  /** Economic amount in the activity/account currency. Card original is separate. */
  readonly amount: string
  readonly currency: string
  readonly settlementDate?: string
  readonly fee?: string
  readonly tax?: string
  /** Provider rate converting activity currency into account/settlement currency. */
  readonly fxRate?: string
  readonly sourceType?: string
  readonly subtype?: string
  readonly sourceRecordId?: string
  readonly sourceGroupId?: string
  /** Provider-owned JSON evidence; never user categories, notes, or overrides. */
  readonly metadata?: Readonly<Record<string, unknown>>
  readonly originalAmount?: string
  readonly originalCurrency?: string
  readonly settlementAmount?: string
  readonly settlementCurrency?: string
  readonly notes?: string
  readonly activityTypeOverride?: string
  readonly needsReview?: boolean
  /** Versioned provider evidence; never a receiver-side category or user note. */
  readonly merchantContext?: Readonly<Record<string, unknown>>
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
  readonly currentState?: CurrentStatePayload
  /** Present for CurrentStateUpsert and computed from its complete payload. */
  readonly semanticHash?: string
  readonly lifecycleSuccessorId?: string
  /** Provider identity corrections that must be atomically merged into this resource. */
  readonly supersededResourceIds?: readonly string[]
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
  readonly bindingVersion: 3
  readonly receiptBinding: string
  readonly semanticHash: string | null
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
  return `v3;${fields.map(field => `${encoder.encode(field).byteLength}:${field}`).join('')}`
}

export function validateBridgeReceipt(operation: BridgeDisclosureOperation, value: unknown): BridgeReceipt {
  assertCurrentStateBinding(operation)
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid bridge receipt')
  const receipt = value as Record<string, unknown>
  if (receipt.outcome !== 'Accepted') throw new Error('bridge receipt rejected')
  if (receipt.operation !== WIRE_OPERATION[operation.kind]) throw new Error('Bridge operation mismatch')
  if (receipt.subject !== operation.subject) throw new Error('Bridge subject mismatch')
  if (receipt.idempotencyKey !== operation.idempotencyKey || receipt.resourceId !== operation.resourceId || receipt.accessGeneration !== operation.accessGeneration) {
    throw new Error('Bridge receipt mismatch')
  }
  const successor = operation.lifecycleSuccessorId ?? null
  const semanticHash = operation.kind === 'CurrentStateUpsert' ? operation.semanticHash ?? null : null
  if (receipt.deploymentId !== operation.deploymentId || receipt.accountId !== operation.accountId || receipt.lifecycleSuccessorId !== successor
    || receipt.bindingVersion !== 3 || receipt.semanticHash !== semanticHash) {
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
    successor === null ? '0' : '1',
    successor ?? '',
    'accepted',
    semanticHash === null ? '0' : '1',
    semanticHash ?? '',
  ])
  if (receipt.receiptBinding !== expected) throw new Error('Bridge receipt binding mismatch')
  return receipt as unknown as BridgeReceipt
}

function merchantContextToWire(value: Readonly<Record<string, unknown>> | undefined): { merchant_context?: Readonly<Record<string, unknown>> } {
  if (value === undefined || value.merchantKey === null) return {}
  if (typeof value.merchantKey !== 'string' || !value.merchantKey || value.merchantKeyVersion !== 1 || value.normalizationVersion !== 1) {
    throw new Error('invalid merchant context')
  }
  return { merchant_context: {
    merchant_key: value.merchantKey,
    merchant_key_version: value.merchantKeyVersion,
    normalization_version: value.normalizationVersion,
  } }
}

export function bridgeOperationToWire(operation: BridgeDisclosureOperation): Record<string, unknown> {
  assertCurrentStateBinding(operation)
  const activity = operation.activity && {
    account_id: operation.activity.accountId,
    activity_type: operation.activity.activityType,
    status: operation.activity.status,
    activity_date: operation.activity.activityDate,
    activity_date_precision: operation.activity.activityDatePrecision,
    amount: operation.activity.amount,
    currency: operation.activity.currency,
    ...(operation.activity.settlementDate === undefined ? {} : { settlement_date: operation.activity.settlementDate }),
    ...(operation.activity.fee === undefined ? {} : { fee: operation.activity.fee }),
    ...(operation.activity.tax === undefined ? {} : { tax: operation.activity.tax }),
    ...(operation.activity.fxRate === undefined ? {} : { fx_rate: operation.activity.fxRate }),
    ...(operation.activity.sourceType === undefined ? {} : { source_type: operation.activity.sourceType }),
    ...(operation.activity.subtype === undefined ? {} : { subtype: operation.activity.subtype }),
    ...(operation.activity.sourceRecordId === undefined ? {} : { source_record_id: operation.activity.sourceRecordId }),
    ...(operation.activity.sourceGroupId === undefined ? {} : { source_group_id: operation.activity.sourceGroupId }),
    ...(operation.activity.metadata === undefined ? {} : { metadata: operation.activity.metadata }),
    ...(operation.activity.originalAmount === undefined ? {} : { original_amount: operation.activity.originalAmount }),
    ...(operation.activity.originalCurrency === undefined ? {} : { original_currency: operation.activity.originalCurrency }),
    ...(operation.activity.settlementAmount === undefined ? {} : { settlement_amount: operation.activity.settlementAmount }),
    ...(operation.activity.settlementCurrency === undefined ? {} : { settlement_currency: operation.activity.settlementCurrency }),
    ...(operation.activity.notes === undefined ? {} : { notes: operation.activity.notes }),
    ...(operation.activity.activityTypeOverride === undefined ? {} : { activity_type_override: operation.activity.activityTypeOverride }),
    ...(operation.activity.needsReview === undefined ? {} : { needs_review: operation.activity.needsReview }),
    ...merchantContextToWire(operation.activity.merchantContext),
  }
  return {
    idempotency_key: operation.idempotencyKey,
    subject: operation.subject,
    access_generation: operation.accessGeneration,
    kind: REQUEST_KIND[operation.kind],
    resource_id: operation.resourceId,
    ...(activity === undefined ? {} : { activity }),
    ...(operation.currentState === undefined ? {} : { current_state: {
      account_id: operation.currentState.accountId,
      provider_connection_id: operation.currentState.providerConnectionId,
      provider_incarnation_id: operation.currentState.providerIncarnationId,
      provider_generation: operation.currentState.providerGeneration,
      observed_at: operation.currentState.observedAt,
      revision: operation.currentState.revision,
      as_of_date: operation.currentState.asOfDate,
      currency: operation.currentState.currency,
      cash_balance: operation.currentState.cashBalance,
      liability_balance: operation.currentState.liabilityBalance,
      positions: operation.currentState.positions.map(position => ({
        symbol: position.symbol, quantity: position.quantity, price: position.price,
        market_value: position.marketValue, currency: position.currency,
        ...(position.costBasis === undefined ? {} : { cost_basis: position.costBasis }),
      })),
    } }),
    ...(operation.semanticHash === undefined ? {} : { semantic_hash: operation.semanticHash }),
    ...(operation.lifecycleSuccessorId === undefined ? {} : { lifecycle_successor_id: operation.lifecycleSuccessorId }),
    ...(operation.supersededResourceIds === undefined || operation.supersededResourceIds.length === 0
      ? {}
      : { superseded_resource_ids: [...operation.supersededResourceIds].sort() }),
  }
}

export type ObservationToBridge = (observation: {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly semanticHash: string
  readonly domain: string
  readonly payload: Readonly<Record<string, unknown>>
  readonly observedAt: string
  readonly supersededResourceIds?: readonly string[]
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

interface DecimalParts {
  readonly magnitude: string
  readonly negative: boolean
}

function decimalParts(value: unknown, allowZero = false): DecimalParts {
  const text = typeof value === 'number'
    ? Number.isFinite(value) ? String(value) : ''
    : typeof value === 'string' ? value.trim() : ''
  if (!/^[+-]?\d+(?:\.\d+)?$/.test(text)) throw new Error('invalid provider amount')
  const negative = text.startsWith('-')
  const magnitude = text.replace(/^[+-]/, '')
  if (!allowZero && !/[1-9]/.test(magnitude.replace('.', ''))) throw new Error('invalid provider amount')
  return { magnitude, negative }
}

function negateDecimal(value: unknown): string {
  const parts = decimalParts(value)
  return parts.negative ? parts.magnitude : `-${parts.magnitude}`
}

function providerDescription(payload: Readonly<Record<string, unknown>>): string | undefined {
  const context = isRecord(payload.merchantContext) ? payload.merchantContext : undefined
  const candidates = [
    context?.merchantDisplay,
    context?.rawDescription,
    payload.description,
    payload.counterpartyName,
    payload.name,
    payload.bankDescription,
    payload.reference,
  ]
  const description = candidates.find(value => typeof value === 'string' && value.trim().length > 0)
  return typeof description === 'string' ? description.trim().slice(0, 512) : undefined
}

function optionalString(payload: Readonly<Record<string, unknown>>, key: string): string | undefined {
  const value = payload[key]
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined
}

function absoluteDecimal(value: unknown): string {
  const parts = decimalParts(value, true)
  return parts.magnitude
}

export function providerActivityDate(payload: Readonly<Record<string, unknown>>): { value: string; precision: 'DATE' | 'INSTANT' } {
  const value = optionalString(payload, 'authorizationDateTime') ?? optionalString(payload, 'date')
  if (!value) throw new Error('provider activity date is required')
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const date = new Date(`${value}T00:00:00Z`)
    if (date.toISOString().slice(0, 10) === value) return { value, precision: 'DATE' }
  }
  if (/^\d{4}-\d{2}-\d{2}T/.test(value) && Number.isFinite(Date.parse(value))) {
    return { value, precision: 'INSTANT' }
  }
  throw new Error('invalid provider activity date')
}

function optionalCurrency(payload: Readonly<Record<string, unknown>>, key: string): string | undefined {
  const value = optionalString(payload, key)?.toUpperCase()
  return value && /^[A-Z]{3}$/.test(value) ? value : undefined
}

export function cashObservationToBridge(
  observation: {
    readonly deploymentId: string
    readonly localOwnerId: string
    readonly sourceResourceIncarnationId: string
    readonly semanticHash: string
    readonly payload: Readonly<Record<string, unknown>>
    readonly observedAt: string
    readonly supersededResourceIds?: readonly string[]
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
      // Re-project existing provider facts once when their display description
      // becomes part of the native activity payload.
      'merchant-notes-v1',
      'activity-date-precision-v1',
      ...(observation.supersededResourceIds ?? []).slice().sort(),
    ]),
    subject,
    accessGeneration,
    ...(observation.supersededResourceIds === undefined || observation.supersededResourceIds.length === 0
      ? {}
      : { supersededResourceIds: [...observation.supersededResourceIds].sort() }),
  }
  if (p.isRemoved === true) {
    return { ...common, kind: 'ActivityDelete', resourceId }
  }
  const parsedAmount = decimalParts(p.amount)
  if (typeof p.currency !== 'string' || p.currency.length === 0) throw new Error('invalid provider currency')
  const currency = p.currency.toUpperCase()
  const providerStatus = typeof p.status === 'string' ? p.status.toUpperCase() : undefined
  const isPending = p.pending === true || providerStatus === 'PENDING'
  const explicitActivityType = optionalString(p, 'activityType')
  const activityType = explicitActivityType ?? (parsedAmount.negative ? 'DEPOSIT' : 'WITHDRAWAL')
  const originalAmount = p.originalAmount === undefined ? undefined : absoluteDecimal(p.originalAmount)
  const originalCurrency = optionalCurrency(p, 'originalCurrency')
  const settlementAmount = p.settlementAmount === undefined ? undefined : absoluteDecimal(p.settlementAmount)
  const settlementCurrency = optionalCurrency(p, 'settlementCurrency')
  const sourceRecordId = optionalString(p, 'sourceRecordId') ?? transactionId
  const activityDate = providerActivityDate(p)
  return {
    ...common,
    kind: 'ActivityUpsert',
    resourceId: pendingResourceId ?? resourceId,
    ...(pendingResourceId === undefined ? {} : { lifecycleSuccessorId: resourceId }),
    activity: {
      accountId,
      activityType,
      status: isPending ? 'PENDING' : 'POSTED',
      // Preserve real card authorization time; account feeds that only provide
      // a date remain date-only rather than receiving a fabricated timestamp.
      activityDate: activityDate.value,
      activityDatePrecision: activityDate.precision,
      amount: parsedAmount.magnitude,
      currency,
      ...(optionalString(p, 'settlementDate') === undefined ? {} : { settlementDate: optionalString(p, 'settlementDate') }),
      ...(optionalString(p, 'fee') === undefined ? {} : { fee: absoluteDecimal(p.fee) }),
      ...(optionalString(p, 'tax') === undefined ? {} : { tax: absoluteDecimal(p.tax) }),
      ...(optionalString(p, 'fxRate') === undefined ? {} : { fxRate: optionalString(p, 'fxRate') }),
      ...(optionalString(p, 'sourceType') === undefined ? {} : { sourceType: optionalString(p, 'sourceType') }),
      ...(optionalString(p, 'subtype') === undefined ? {} : { subtype: optionalString(p, 'subtype') }),
      sourceRecordId,
      ...(optionalString(p, 'sourceGroupId') === undefined ? {} : { sourceGroupId: optionalString(p, 'sourceGroupId') }),
      ...(isRecord(p.metadata) ? { metadata: p.metadata } : {}),
      ...(originalAmount === undefined ? {} : { originalAmount }),
      ...(originalCurrency === undefined ? {} : { originalCurrency }),
      ...(settlementAmount === undefined ? {} : { settlementAmount }),
      ...(settlementCurrency === undefined ? {} : { settlementCurrency }),
      ...(p.needsReview === true ? { needsReview: true } : {}),
      ...(providerDescription(p) === undefined ? {} : { notes: providerDescription(p) }),
      ...(isRecord(p.merchantContext) ? { merchantContext: p.merchantContext } : {}),
    },
  }
}

function assertCurrentStateBinding(operation: BridgeDisclosureOperation): void {
  if (operation.kind !== 'CurrentStateUpsert') return
  if (!operation.currentState || !operation.semanticHash || currentStateSemanticHash(operation.currentState) !== operation.semanticHash) {
    throw new Error('current-state payload binding mismatch')
  }
}

export function currentStateToBridge(
  context: {
    readonly deploymentId: string
    readonly localOwnerId: string
    readonly sourceResourceIncarnationId: string
  },
  payload: CurrentStatePayload,
  semanticHash: string,
  accessGeneration: number,
  connectionId = context.sourceResourceIncarnationId,
): BridgeDisclosureOperation {
  if (currentStateSemanticHash(payload) !== semanticHash) throw new Error('current-state semantic hash mismatch')
  const subject = assertIssuerQualifiedSubject(context.localOwnerId)
  const resourceId = `${context.deploymentId}:${context.sourceResourceIncarnationId}:current-state:${payload.accountId}`
  return {
    deploymentId: context.deploymentId,
    accountId: payload.accountId,
    idempotencyKey: idempotencyKey(context.deploymentId, semanticHash, [subject, connectionId, context.sourceResourceIncarnationId, payload.accountId, resourceId, 'CurrentStateUpsert']),
    subject,
    accessGeneration,
    kind: 'CurrentStateUpsert',
    resourceId,
    currentState: payload,
    semanticHash,
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
    readonly supersededResourceIds?: readonly string[]
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
  // Provider removals remain immutable canonical evidence until a separate
  // resolution/grace policy proves the visible activity should be deleted.
  if (payload.isRemoved === true) return null

  const parsedAmount = decimalParts(payload.amount)
  if (provider === 'plaid') return cashObservationToBridge(observation, accessGeneration, accountId, connectionId)
  if (provider === 'investec' || provider === 'mercury') {
    // Direct-bank adapters use ledger signs (negative money-out); Plaid uses the inverse.
    // Pocket funding is an internal transfer, never a spending withdrawal.
    const transactionType = optionalString(payload, 'transactionType')?.toUpperCase()
    const direction = (optionalString(payload, 'type') ?? optionalString(payload, 'direction'))?.toUpperCase()
    const isPocketTransfer = provider === 'investec'
      && (transactionType === 'MCPBAPOCKETTRANSFERS' || transactionType?.includes('POCKET'))
    const activityType = isPocketTransfer
      ? (direction === 'CREDIT' ? 'TRANSFER_IN' : 'TRANSFER_OUT')
      : undefined
    return cashObservationToBridge({
      ...observation,
      payload: {
        ...payload,
        amount: negateDecimal(payload.amount),
        ...(activityType === undefined ? {} : { activityType }),
        ...(optionalString(payload, 'sourceType') === undefined && transactionType === undefined
          ? {}
          : { sourceType: optionalString(payload, 'sourceType') ?? transactionType }),
      },
    }, accessGeneration, accountId, connectionId)
  }
  if (observation.domain !== 'brokerage_activity') return null
  const activityTypes: Readonly<Record<string, string>> = {
    buy: 'BUY', sell: 'SELL', dividend: 'DIVIDEND', contribution: 'DEPOSIT',
    withdrawal: 'WITHDRAWAL', transfer: 'TRANSFER', fee: 'FEE', interest: 'INTEREST',
  }
  const activityType = activityTypes[String(payload.type)]
  if (!activityType) throw new Error('unsupported brokerage activity type')
  const base = cashObservationToBridge({ ...observation, payload: { ...payload, amount: negateDecimal(payload.amount) } }, accessGeneration, accountId, connectionId)
  if (!base.activity) throw new Error('brokerage activity projection failed')
  return { ...base, activity: { ...base.activity, activityType, amount: parsedAmount.magnitude, status: 'POSTED' } }
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
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
