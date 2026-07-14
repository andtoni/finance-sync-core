import { describe, expect, expectTypeOf, it } from 'vitest'
import {
  canonicalEntityId,
  deploymentId,
  localOwnerId,
  providerPrincipalId,
  providerResourceIncarnationId,
  sourceAccountIncarnationId,
  sourceConnectionIncarnationId,
  type DeploymentId,
  type LocalOwnerId,
} from './identity'

describe('Finance Sync identity', () => {
  it('constructs each identity as a distinct branded value', () => {
    const deployment = deploymentId('dep-1')
    const owner = localOwnerId('owner-1')
    expect(deployment).toBe('dep-1')
    expect(owner).toBe('owner-1')
    expect(providerPrincipalId('principal-1')).toBe('principal-1')
    expect(sourceConnectionIncarnationId('connection-1')).toBe('connection-1')
    expect(sourceAccountIncarnationId('account-1')).toBe('account-1')
    expect(providerResourceIncarnationId('resource-1')).toBe('resource-1')
    expect(canonicalEntityId('entity-1')).toBe('entity-1')
    expectTypeOf<DeploymentId>().not.toEqualTypeOf<LocalOwnerId>()
  })

  it.each(['', '   ', 'has space', 'bad/control\n', 'x'.repeat(129)])('rejects invalid identity %j', value => {
    expect(() => deploymentId(value)).toThrowError('INVALID_DEPLOYMENT_ID')
  })
})
