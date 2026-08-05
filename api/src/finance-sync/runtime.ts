import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import { getProviderEntry, type ProviderName } from './probes/provider-registry'

interface RuntimeConnectionBase {
  readonly provider: ProviderName
  readonly connectionId: string
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly accountIds: readonly string[]
  readonly wealthfolioAccountIds: Readonly<Record<string, string>>
  /** Provider credential/connection epoch used for ingestion fencing and provenance. */
  readonly providerGeneration: number
  /** Wealthfolio grant epoch used only for receiver bridge authorization. */
  readonly receiverAccessGeneration: number
  /** Non-secret Infisical slot reference for active-connection allocation exclusion. */
  readonly secretReference: string
}

export interface PlaidRuntimeConnection extends RuntimeConnectionBase {
  readonly provider: 'plaid'
  /** All Plaid credentials are resolved from the durable Infisical bundle. */
  readonly clientId: string
  readonly clientSecret: string
  readonly accessToken: string
  readonly maxPages?: number
}

export interface InvestecRuntimeConnection extends RuntimeConnectionBase {
  readonly provider: 'investec'
  readonly clientId: string
  readonly clientSecret: string
  readonly apiKey: string
  readonly eligibleProductNames: readonly string[]
  readonly fromDate: string
  readonly toDate: string
  readonly maxPages?: number
}

export interface MercuryRuntimeConnection extends RuntimeConnectionBase {
  readonly provider: 'mercury'
  readonly apiToken: string
  readonly since?: string
  readonly maxPagesPerAccount?: number
}

export type SnapTradeAuthMode = 'personal' | 'commercial'

export interface SnapTradeRuntimeConnection extends RuntimeConnectionBase {
  readonly provider: 'snaptrade'
  readonly clientId: string
  readonly consumerKey: string
  /** Personal keys identify the SnapTrade user and omit subordinate credentials. */
  readonly authMode: SnapTradeAuthMode
  readonly userId?: string
  readonly userSecret?: string
  readonly assetMappings: Readonly<Record<string, string>>
  readonly activitiesStartDate?: string
  readonly activitiesEndDate?: string
}

export type RuntimeConnection =
  | PlaidRuntimeConnection
  | InvestecRuntimeConnection
  | MercuryRuntimeConnection
  | SnapTradeRuntimeConnection

export interface ProviderRun { readonly nextCursor: string | null; readonly observations: readonly ObservationOutput[] }
export interface PersistEnqueueOptions { readonly skipCurrentState?: boolean }
export interface SyncRuntimeDependencies {
  loadConnections(deploymentId: string): Promise<readonly RuntimeConnection[]>
  loadCheckpoint(connection: RuntimeConnection): Promise<string | null>
  runProvider(connection: RuntimeConnection, cursor: string | null): Promise<ProviderRun>
  persistAndEnqueue(connection: RuntimeConnection, observations: readonly ObservationOutput[], options?: PersistEnqueueOptions): Promise<number>
  projectPending(connection: RuntimeConnection): Promise<{ delivered: number; failed: number }>
  saveCheckpoint(connection: RuntimeConnection, cursor: string | null): Promise<void>
}
export interface SyncCycleResult { readonly connections: number; readonly observations: number; readonly delivered: number; readonly failures: number }
export interface SyncCycleControls { readonly providersEnabled: boolean; readonly projectionEnabled: boolean }

const enabledByDefault: SyncCycleControls = Object.freeze({ providersEnabled: true, projectionEnabled: true })

/** Read-only provider ingestion. With projection enabled, cursor promotion waits for full outbox delivery. */
export async function runConfiguredSyncCycle(
  deploymentId: string,
  deps: SyncRuntimeDependencies,
  controls: SyncCycleControls = enabledByDefault,
): Promise<SyncCycleResult> {
  if (!controls.providersEnabled) return { connections: 0, observations: 0, delivered: 0, failures: 0 }
  const connections = await deps.loadConnections(deploymentId)
  let observations = 0; let delivered = 0; let failures = 0
  for (const connection of connections) {
    getProviderEntry(connection.provider)
    try {
      const cursor = await deps.loadCheckpoint(connection)
      const run = await deps.runProvider(connection, cursor)
      const inserted = await deps.persistAndEnqueue(connection, run.observations)
      if (!controls.projectionEnabled) {
        observations += inserted
        await deps.saveCheckpoint(connection, run.nextCursor)
        continue
      }
      const projection = await deps.projectPending(connection)
      observations += inserted; delivered += projection.delivered; failures += projection.failed
      if (projection.failed === 0) await deps.saveCheckpoint(connection, run.nextCursor)
    } catch {
      failures += 1
      // Provider errors can embed tokens, identifiers, request payloads, and financial data.
      console.error('Finance Sync connection cycle failed')
    }
  }
  return { connections: connections.length, observations, delivered, failures }
}
