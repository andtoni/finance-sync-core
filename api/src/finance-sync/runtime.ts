import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import { getProviderEntry, type ProviderName } from './probes/provider-registry'

export interface RuntimeConnection {
  readonly provider: ProviderName
  readonly connectionId: string
  readonly deploymentId: string
  readonly accessToken: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly accountIds: readonly string[]
  readonly wealthfolioAccountIds: Readonly<Record<string, string>>
  readonly accessGeneration: number
}
export interface ProviderRun { readonly nextCursor: string | null; readonly observations: readonly ObservationOutput[] }
export interface SyncRuntimeDependencies {
  loadConnections(deploymentId: string): Promise<readonly RuntimeConnection[]>
  loadCheckpoint(connection: RuntimeConnection): Promise<string | null>
  runProvider(connection: RuntimeConnection, cursor: string | null): Promise<ProviderRun>
  persistObservations(observations: readonly ObservationOutput[]): Promise<readonly ObservationOutput[]>
  enqueue(observations: readonly ObservationOutput[]): Promise<void>
  projectPending(): Promise<{ delivered: number; failed: number }>
  saveCheckpoint(connection: RuntimeConnection, cursor: string | null): Promise<void>
}
export interface SyncCycleResult { readonly connections: number; readonly observations: number; readonly delivered: number; readonly failures: number }

/** Read-only provider ingestion. Cursor promotion happens only after its outbox is fully delivered. */
export async function runConfiguredSyncCycle(deploymentId: string, deps: SyncRuntimeDependencies): Promise<SyncCycleResult> {
  const connections = await deps.loadConnections(deploymentId)
  let observations = 0; let delivered = 0; let failures = 0
  for (const connection of connections) {
    getProviderEntry(connection.provider)
    try {
      const cursor = await deps.loadCheckpoint(connection)
      const run = await deps.runProvider(connection, cursor)
      const inserted = await deps.persistObservations(run.observations)
      await deps.enqueue(inserted)
      const projection = await deps.projectPending()
      observations += inserted.length; delivered += projection.delivered; failures += projection.failed
      if (projection.failed === 0) await deps.saveCheckpoint(connection, run.nextCursor)
    } catch {
      failures += 1
      // Provider errors can embed tokens, identifiers, request payloads, and financial data.
      // Emit only a bounded operational signal; detailed provider evidence is separately redacted.
      console.error('Finance Sync connection cycle failed')
    }
  }
  return { connections: connections.length, observations, delivered, failures }
}
