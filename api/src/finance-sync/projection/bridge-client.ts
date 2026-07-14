
import { createHash } from 'node:crypto'

export type BridgeDisclosureKind = 'ActivityUpsert' | 'ActivityDelete' | 'ProjectionFence'

export interface BridgeActivityPayload {
  readonly accountId: string
  readonly activityType: string
  readonly status: string
  readonly activityDate: string
  readonly amount: string
  readonly currency: string
  readonly notes?: string
  readonly activityTypeOverride?: string
  readonly needsReview?: boolean
}

export interface BridgeDisclosureOperation {
  readonly idempotencyKey: string
  readonly subject: string
  readonly accessGeneration: number
  readonly kind: BridgeDisclosureKind
  readonly resourceId: string
  readonly activity?: BridgeActivityPayload
  readonly lifecycleSuccessorId?: string
}

export interface BridgeReceipt {
  readonly subject: string
  readonly idempotencyKey: string
  readonly accessGeneration: number
  readonly operation: string
  readonly resourceId: string
  readonly outcome: 'Accepted' | 'Revoked'
}

export interface BridgeTransport {
  commit(op: BridgeDisclosureOperation): Promise<BridgeReceipt>
}

export type ObservationToBridge = (observation: {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly semanticHash: string
  readonly domain: string
  readonly payload: Readonly<Record<string, unknown>>
  readonly observedAt: string
}) => BridgeDisclosureOperation

export function idempotencyKey(deploymentId: string, semanticHash: string): string {
  return createHash('sha256').update(`${deploymentId}:${semanticHash}`, 'utf8').digest('hex').slice(0, 40)
}

export function cashObservationToBridge(
  observation: {
    readonly deploymentId: string
    readonly localOwnerId: string
    readonly sourceResourceIncarnationId: string
    readonly semanticHash: string
    readonly payload: Readonly<Record<string, unknown>>
    readonly observedAt: string
  },
  accessGeneration: number,
  accountId: string,
): BridgeDisclosureOperation {
  const p = observation.payload as Record<string, unknown>
  return {
    idempotencyKey: idempotencyKey(observation.deploymentId, observation.semanticHash),
    subject: observation.localOwnerId,
    accessGeneration,
    kind: 'ActivityUpsert',
    resourceId: `${observation.deploymentId}:${observation.sourceResourceIncarnationId}:${p.transactionId ?? p.transaction_id ?? observation.semanticHash}`,
    activity: {
      accountId,
      activityType: (p.isRemoved as boolean) ? 'REMOVAL' : String(p.amount ?? '0').startsWith('-') ? 'WITHDRAWAL' : 'DEPOSIT',
      status: (p.isRemoved as boolean) ? 'VOID' : 'CLEARED',
      activityDate: (p.date as string) ?? observation.observedAt.slice(0, 10),
      amount: String(p.amount ?? '0').replace(/^-/, ''),
      currency: (p.currency as string) ?? 'USD',
      notes: [p.name as string, p.description as string].filter(Boolean).join(' | ') || undefined,
      activityTypeOverride: (p.transactionType as string) === 'CREDIT' ? 'DEPOSIT' : (p.transactionType as string) === 'DEBIT' ? 'WITHDRAWAL' : undefined,
      needsReview: (p.pending as boolean) ?? false,
    },
  }
}

export async function commitObservation(
  transport: BridgeTransport,
  operation: BridgeDisclosureOperation,
  maxRetries = 3,
): Promise<BridgeReceipt> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const receipt = await transport.commit(operation)
      if (receipt.outcome !== 'Accepted') {
        throw new Error(`Bridge rejected: ${receipt.outcome} for ${receipt.idempotencyKey}`)
      }
      return receipt
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
