import type { CanonicalEntityId, DeploymentId, LocalOwnerId, ProviderResourceIncarnationId } from './identity'
import { localOwnerId as loId, providerResourceIncarnationId as prId, deploymentId as depId, canonicalEntityId as ceId } from './identity'
import type { Observation, CanonicalRevision } from './observations/lifecycle'
import { appendObservation, canonicalRevision } from './observations/lifecycle'

// ─── G1 pipeline: observation → canonical revision → outbox → receipt ─────────

export const G1_SCHEMA_VERSION = 1 as const
export type G1Operation = 'upsert_activity' | 'remove_activity' | 'supersede_pending'
export type ReceiptOutcome = 'created' | 'updated' | 'removed' | 'superseded' | 'duplicate'

// ─── Types ────────────────────────────────────────────────────────────────────

export interface PipelineIdentity {
  deploymentId: DeploymentId
  localOwnerId: LocalOwnerId
  canonicalEntityId: CanonicalEntityId
  sourceResourceIncarnationId: ProviderResourceIncarnationId
}

export interface OutboxEntry {
  readonly canonicalRevision: number
  readonly operation: G1Operation
  readonly semanticHash: string
  readonly mappingVersion: string
  readonly payload: Readonly<Record<string, unknown>>
  readonly createdAt: string
}

export interface Receipt {
  readonly canonicalRevision: number
  readonly targetId: string
  readonly outcome: ReceiptOutcome
  readonly recordedAt: string
}

export interface UserOverlay {
  readonly category: string | null
  readonly note: string | null
  readonly revision: number
  readonly updatedAt: string
}

export interface PipelineState {
  readonly identity: PipelineIdentity
  readonly observations: readonly Observation[]
  readonly canonical: CanonicalRevision | null
  readonly outbox: readonly OutboxEntry[]
  readonly receipts: readonly Receipt[]
  readonly userOverlay: UserOverlay | null
}

// ─── Pipeline operations ──────────────────────────────────────────────────────

export function g1IngestObservation(
  state: PipelineState,
  payload: Readonly<Record<string, unknown>>,
  semanticHash: string,
  providerVersion: string | null,
  observedAt: string,
): PipelineState {
  const { identity } = state
  const observation: Observation = {
    identity: {
      deploymentId: identity.deploymentId,
      localOwnerId: identity.localOwnerId,
      canonicalEntityId: identity.canonicalEntityId,
      providerResourceIncarnationId: identity.sourceResourceIncarnationId,
    },
    observedAt,
    providerVersion: providerVersion ?? '',
    rawHash: semanticHash,
    payload: Object.freeze({ ...payload }),
  }
  return { ...state, observations: appendObservation(state.observations, observation) }
}

export function g1ProduceRevision(
  state: PipelineState,
  semanticHash: string,
  mappingVersion: string,
): PipelineState {
  const { identity } = state
  const revIdentity = {
    deploymentId: identity.deploymentId,
    localOwnerId: identity.localOwnerId,
    canonicalEntityId: identity.canonicalEntityId,
    providerResourceIncarnationId: identity.sourceResourceIncarnationId,
  }
  return { ...state, canonical: canonicalRevision(state.canonical, revIdentity, semanticHash, mappingVersion) }
}

export function g1EnqueueOutbox(
  state: PipelineState,
  operation: G1Operation,
  payload: Readonly<Record<string, unknown>>,
): PipelineState {
  if (!state.canonical) throw new Error('no canonical revision to enqueue')
  return {
    ...state,
    outbox: Object.freeze([
      ...state.outbox,
      {
        canonicalRevision: state.canonical.revision,
        operation,
        semanticHash: state.canonical.semanticHash,
        mappingVersion: state.canonical.mappingVersion,
        payload: Object.freeze({ ...payload }),
        createdAt: new Date().toISOString(),
      },
    ]),
  }
}

export function g1RecordReceipt(
  state: PipelineState,
  targetId: string,
  outcome: ReceiptOutcome,
): PipelineState {
  const latest = state.outbox[state.outbox.length - 1]
  if (!latest) throw new Error('no outbox entry to confirm')
  return {
    ...state,
    receipts: Object.freeze([
      ...state.receipts,
      {
        canonicalRevision: latest.canonicalRevision,
        targetId,
        outcome,
        recordedAt: new Date().toISOString(),
      },
    ]),
  }
}

export function g1ApplyEdit(
  state: PipelineState,
  category: string | null,
  note: string | null,
): PipelineState {
  const prior = state.userOverlay
  if (prior && category === prior.category && note === prior.note) return state
  return {
    ...state,
    userOverlay: Object.freeze({
      category,
      note,
      revision: (prior?.revision ?? 0) + 1,
      updatedAt: new Date().toISOString(),
    }),
  }
}

/**
 * Provider correction: new canonical revision + outbox, preserve user overlay.
 */
export function g1CorrectProvider(
  state: PipelineState,
  payload: Readonly<Record<string, unknown>>,
  newHash: string,
  mappingVersion: string,
): PipelineState {
  let s = g1ProduceRevision(state, newHash, mappingVersion)
  s = g1EnqueueOutbox(s, 'upsert_activity', payload)
  return s
}

/**
 * Removal: final revision + outbox for tombstone.
 */
export function g1Remove(
  state: PipelineState,
  mappingVersion: string,
): PipelineState {
  let s = g1ProduceRevision(state, `00000000000000000000000000000000${'f'.repeat(32)}`, mappingVersion)
  s = g1EnqueueOutbox(s, 'remove_activity', { operation: 'remove', removedAt: new Date().toISOString() })
  return s
}

/**
 * Create a fresh pipeline state for one canonical entity.
 */
export function g1FreshPipeline(
  deploymentIdStr: string,
  localOwnerIdStr: string,
  entityId: string,
  sourceResourceId: string,
): PipelineState {
  return {
    identity: {
      deploymentId: depId(deploymentIdStr),
      localOwnerId: loId(localOwnerIdStr),
      canonicalEntityId: ceId(entityId),
      sourceResourceIncarnationId: prId(sourceResourceId),
    },
    observations: Object.freeze([]),
    canonical: null,
    outbox: Object.freeze([]),
    receipts: Object.freeze([]),
    userOverlay: null,
  }
}
