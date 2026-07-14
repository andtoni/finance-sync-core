export const SOURCE_RESOURCE_STATES = [
  'present', 'provider_removed', 'provider_deleted_confirmed', 'missing_unconfirmed',
  'access_lost', 'superseded', 'reappeared',
] as const
export type SourceResourceStateValue = (typeof SOURCE_RESOURCE_STATES)[number]

export const ECONOMIC_STATES = [
  'provisional', 'pending', 'booked', 'failed_nonposting', 'cancelled_nonposting',
  'blocked_nonposting', 'reversed_economic', 'void', 'removed_unresolved', 'removed_visible',
] as const
export type EconomicStateValue = (typeof ECONOMIC_STATES)[number]

export const PROJECTION_STATES = ['active', 'superseded', 'tombstoned', 'rejected'] as const
export type ProjectionStateValue = (typeof PROJECTION_STATES)[number]

export interface SourceResourceState { readonly value: SourceResourceStateValue }
export interface EconomicState { readonly value: EconomicStateValue }
export interface ProjectionState { readonly value: ProjectionStateValue }

function state<T extends string>(value: string, allowed: readonly T[], error: string): Readonly<{ value: T }> {
  if (!allowed.includes(value as T)) throw new Error(error)
  return Object.freeze({ value: value as T })
}

export const sourceResourceState = (value: string): SourceResourceState =>
  state(value, SOURCE_RESOURCE_STATES, 'INVALID_SOURCE_RESOURCE_STATE')
export const economicState = (value: string): EconomicState =>
  state(value, ECONOMIC_STATES, 'INVALID_ECONOMIC_STATE')
export const projectionState = (value: string): ProjectionState =>
  state(value, PROJECTION_STATES, 'INVALID_PROJECTION_STATE')
