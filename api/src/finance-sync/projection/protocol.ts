import type {
  CanonicalEntityId,
  DeploymentId,
  LocalOwnerId,
} from '../identity'

export const BRIDGE_SCHEMA_VERSION = 1 as const

export type BridgeOperation =
  | 'upsert_account'
  | 'remove_account'
  | 'upsert_activity'
  | 'remove_activity'
  | 'supersede_pending_activity'

export interface ProjectionIdentity {
  readonly deploymentId: DeploymentId
  readonly localOwnerId: LocalOwnerId
  readonly canonicalEntityId: CanonicalEntityId
}

export interface BridgeRequest {
  readonly schemaVersion: typeof BRIDGE_SCHEMA_VERSION
  readonly identity: ProjectionIdentity
  readonly operation: BridgeOperation
  readonly canonicalRevision: number
  readonly expectedPriorRevision: number | null
  readonly payload: Readonly<Record<string, unknown>>
}

export interface BridgeReceipt {
  readonly schemaVersion: typeof BRIDGE_SCHEMA_VERSION
  readonly deploymentId: DeploymentId
  readonly localOwnerId: LocalOwnerId
  readonly canonicalEntityId: CanonicalEntityId
  readonly acceptedRevision: number
  readonly targetId: string
  readonly outcome: 'created' | 'updated' | 'removed' | 'superseded' | 'duplicate'
}

export function bridgeRequest(input: Omit<BridgeRequest, 'schemaVersion'>): BridgeRequest {
  if (!Number.isSafeInteger(input.canonicalRevision) || input.canonicalRevision < 1) {
    throw new Error('INVALID canonicalRevision')
  }
  if (input.expectedPriorRevision !== null && (
    !Number.isSafeInteger(input.expectedPriorRevision)
    || input.expectedPriorRevision < 1
    || input.expectedPriorRevision >= input.canonicalRevision
  )) throw new Error('INVALID expectedPriorRevision')

  return { ...input, schemaVersion: BRIDGE_SCHEMA_VERSION }
}

export function parseBridgeReceipt(request: BridgeRequest, value: unknown): BridgeReceipt {
  if (!isRecord(value)) throw new Error('INVALID bridge receipt')
  if (value.schemaVersion !== request.schemaVersion) throw new Error('INVALID receipt schema')
  if (
    value.deploymentId !== request.identity.deploymentId
    || value.localOwnerId !== request.identity.localOwnerId
    || value.canonicalEntityId !== request.identity.canonicalEntityId
  ) throw new Error('INVALID receipt identity')
  if (value.acceptedRevision !== request.canonicalRevision) throw new Error('INVALID receipt revision')
  if (typeof value.targetId !== 'string' || value.targetId.length === 0) throw new Error('INVALID receipt target')
  if (!['created', 'updated', 'removed', 'superseded', 'duplicate'].includes(String(value.outcome))) {
    throw new Error('INVALID receipt outcome')
  }
  return value as unknown as BridgeReceipt
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}
