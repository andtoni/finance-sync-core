export type CanonicalDomain =
  | 'cash_ledger'
  | 'balance'
  | 'liability'
  | 'brokerage_activity'
  | 'holding'
  | 'security'
  | 'cost_basis'
  | 'order'

export interface ObservationEnvelope {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly providerVersion: string | null
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
}

export interface CashLedgerObservation extends ObservationEnvelope {
  readonly accountCurrency: string | null
  readonly amount: string
  readonly effectiveAt: string
}

export function assertCurrency(currency: string | null): string {
  if (!currency?.trim()) throw new Error('MISSING_ACCOUNT_CURRENCY')
  return currency.trim().toUpperCase()
}

export function canonicalObservationIdentity(observation: ObservationEnvelope): string {
  return [
    observation.deploymentId,
    observation.sourceResourceIncarnationId,
    observation.providerVersion ?? '',
    observation.semanticHash,
    String(observation.hashSpecificationVersion),
  ].join('\u001f')
}
