import { describe, expect, it } from 'vitest'
import { economicState, projectionState, sourceResourceState } from './states'

describe('Finance Sync orthogonal state dimensions', () => {
  it('constructs immutable source, economic and projection states separately', () => {
    const source = sourceResourceState('provider_removed')
    const economic = economicState('reversed_economic')
    const projection = projectionState('tombstoned')
    expect(source).toEqual({ value: 'provider_removed' })
    expect(economic).toEqual({ value: 'reversed_economic' })
    expect(projection).toEqual({ value: 'tombstoned' })
    expect([source, economic, projection].every(Object.isFrozen)).toBe(true)
  })

  it('rejects unknown state instead of coercing it to other', () => {
    expect(() => sourceResourceState('deleted')).toThrowError('INVALID_SOURCE_RESOURCE_STATE')
    expect(() => economicState('other')).toThrowError('INVALID_ECONOMIC_STATE')
    expect(() => projectionState('pending')).toThrowError('INVALID_PROJECTION_STATE')
  })
})
