import type {
  CanonicalEntityId,
  DeploymentId,
  LocalOwnerId,
  ProviderResourceIncarnationId,
} from '../identity'

interface ObservationIdentity {
  readonly deploymentId: DeploymentId
  readonly localOwnerId: LocalOwnerId
  readonly canonicalEntityId: CanonicalEntityId
  readonly providerResourceIncarnationId: ProviderResourceIncarnationId
}

export interface Observation {
  readonly identity: ObservationIdentity
  readonly observedAt: string
  readonly providerVersion: string
  readonly rawHash: string
  readonly payload: Readonly<Record<string, unknown>>
}

export interface CanonicalRevision {
  readonly identity: ObservationIdentity
  readonly revision: number
  readonly semanticHash: string
  readonly mappingVersion: string
}

export function appendObservation(
  observations: readonly Observation[],
  candidate: Observation,
): readonly Observation[] {
  validateHash(candidate.rawHash)
  validateInstant(candidate.observedAt)
  if (observations.some((item) => sameEvidence(item, candidate))) return observations
  return Object.freeze([...observations, freezeObservation(candidate)])
}

export function canonicalRevision(
  prior: CanonicalRevision | null,
  identity: ObservationIdentity,
  semanticHash: string,
  mappingVersion: string,
): CanonicalRevision {
  validateHash(semanticHash)
  if (!mappingVersion) throw new Error('INVALID mappingVersion')
  if (prior && !sameIdentity(prior.identity, identity)) throw new Error('canonical identity mismatch')
  if (prior && prior.semanticHash === semanticHash && prior.mappingVersion === mappingVersion) return prior
  return Object.freeze({
    identity: Object.freeze({ ...identity }),
    revision: (prior?.revision ?? 0) + 1,
    semanticHash,
    mappingVersion,
  })
}

function sameEvidence(a: Observation, b: Observation): boolean {
  return sameIdentity(a.identity, b.identity)
    && a.providerVersion === b.providerVersion
    && a.rawHash === b.rawHash
}

function sameIdentity(a: ObservationIdentity, b: ObservationIdentity): boolean {
  return a.deploymentId === b.deploymentId
    && a.localOwnerId === b.localOwnerId
    && a.canonicalEntityId === b.canonicalEntityId
    && a.providerResourceIncarnationId === b.providerResourceIncarnationId
}

function freezeObservation(value: Observation): Observation {
  return Object.freeze({
    ...value,
    identity: Object.freeze({ ...value.identity }),
    payload: deepFreeze(structuredClone(value.payload)),
  })
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) deepFreeze(child)
    Object.freeze(value)
  }
  return value
}

function validateHash(value: string): void {
  if (!/^[a-f0-9]{64}$/.test(value)) throw new Error('INVALID hash')
}

function validateInstant(value: string): void {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)
    || Number.isNaN(Date.parse(value))) throw new Error('INVALID observedAt')
}
