import { describe, expect, it } from 'vitest'
import { appendObservation, canonicalRevision } from './lifecycle'
import {
  canonicalEntityId,
  deploymentId,
  localOwnerId,
  providerResourceIncarnationId,
} from '../identity'

const identity = {
  deploymentId: deploymentId('deployment-a'),
  localOwnerId: localOwnerId('owner-a'),
  canonicalEntityId: canonicalEntityId('entity-a'),
  providerResourceIncarnationId: providerResourceIncarnationId('resource-incarnation-a'),
}

describe('immutable observation lifecycle', () => {
  it('deduplicates exact evidence but appends changed observations', () => {
    const first = appendObservation([], {
      identity,
      observedAt: '2026-07-11T12:00:00.000Z',
      providerVersion: '2026-01',
      rawHash: 'a'.repeat(64),
      payload: { status: 'posted' },
    })
    const duplicate = appendObservation(first, first[0])
    const changed = appendObservation(duplicate, {
      ...first[0],
      observedAt: '2026-07-11T12:01:00.000Z',
      rawHash: 'b'.repeat(64),
    })

    expect(first).toHaveLength(1)
    expect(duplicate).toBe(first)
    expect(changed).toHaveLength(2)
    expect(Object.isFrozen(changed[0])).toBe(true)
  })

  it('allocates monotonic revisions and requires semantic change', () => {
    const first = canonicalRevision(null, identity, 'a'.repeat(64), 'mapping-v1')
    expect(first.revision).toBe(1)
    expect(canonicalRevision(first, identity, 'a'.repeat(64), 'mapping-v1')).toBe(first)
    expect(canonicalRevision(first, identity, 'b'.repeat(64), 'mapping-v1').revision).toBe(2)
  })

  it('never carries revisions across canonical identities', () => {
    const first = canonicalRevision(null, identity, 'a'.repeat(64), 'mapping-v1')
    expect(() => canonicalRevision(first, {
      ...identity,
      canonicalEntityId: canonicalEntityId('entity-b'),
    }, 'b'.repeat(64), 'mapping-v1')).toThrow('identity')
  })
})
