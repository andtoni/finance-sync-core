import { describe, expect, it } from 'vitest'
import { bridgeRequest, parseBridgeReceipt } from './protocol'
import {
  canonicalEntityId,
  deploymentId,
  localOwnerId,
} from '../identity'

describe('Finance Sync projection protocol', () => {
  const identity = {
    deploymentId: deploymentId('deployment-a'),
    localOwnerId: localOwnerId('owner-a'),
    canonicalEntityId: canonicalEntityId('activity-a'),
  }

  it('requires positive monotonic revisions and an explicit operation', () => {
    expect(() => bridgeRequest({
      identity,
      operation: 'upsert_activity',
      canonicalRevision: 0,
      expectedPriorRevision: null,
      payload: {},
    })).toThrow('canonicalRevision')

    expect(bridgeRequest({
      identity,
      operation: 'upsert_activity',
      canonicalRevision: 1,
      expectedPriorRevision: null,
      payload: { amount: '1.00' },
    })).toMatchObject({ schemaVersion: 1, canonicalRevision: 1 })
  })

  it('rejects malformed, cross-identity or stale receipts', () => {
    const request = bridgeRequest({
      identity,
      operation: 'remove_activity',
      canonicalRevision: 3,
      expectedPriorRevision: 2,
      payload: {},
    })

    expect(() => parseBridgeReceipt(request, {
      schemaVersion: 1,
      deploymentId: 'deployment-b',
      localOwnerId: 'owner-a',
      canonicalEntityId: 'activity-a',
      acceptedRevision: 3,
      targetId: 'wealthfolio-a',
      outcome: 'removed',
    })).toThrow('identity')

    expect(() => parseBridgeReceipt(request, {
      schemaVersion: 1,
      deploymentId: 'deployment-a',
      localOwnerId: 'owner-a',
      canonicalEntityId: 'activity-a',
      acceptedRevision: 2,
      targetId: 'wealthfolio-a',
      outcome: 'removed',
    })).toThrow('revision')
  })
})
