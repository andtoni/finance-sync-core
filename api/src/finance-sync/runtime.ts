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

export interface ProviderRun {
  readonly nextCursor: string | null
  readonly observations: readonly ObservationOutput[]
  /** Transient full account evidence used only for exact cross-account pairing. */
  readonly transferAccounts?: Readonly<Record<string, string>>
}
export interface PersistEnqueueOptions { readonly skipCurrentState?: boolean }
export interface SyncRuntimeDependencies {
  loadConnections(deploymentId: string): Promise<readonly RuntimeConnection[]>
  loadCheckpoint(connection: RuntimeConnection): Promise<string | null>
  runProvider(connection: RuntimeConnection, cursor: string | null): Promise<ProviderRun>
  persistAndEnqueue(connection: RuntimeConnection, observations: readonly ObservationOutput[], options?: PersistEnqueueOptions): Promise<number>
  persistAndEnqueueBatch?(batches: readonly {
    readonly connection: RuntimeConnection
    readonly observations: readonly ObservationOutput[]
    readonly options?: PersistEnqueueOptions
  }[]): Promise<number>
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
  const runs: Array<{ connection: RuntimeConnection; cursor: string | null; run: ProviderRun }> = []
  for (const connection of connections) {
    getProviderEntry(connection.provider)
    try {
      const cursor = await deps.loadCheckpoint(connection)
      const run = await deps.runProvider(connection, cursor)
      runs.push({ connection, cursor, run })
    } catch {
      failures += 1
      // Provider errors can embed tokens, identifiers, request payloads, and financial data.
      console.error('Finance Sync connection cycle failed')
    }
  }
  // Investec durable admissions are one account per runtime connection. Pair
  // only after every expected sibling in the same provider principal has
  // completed, so a failed sibling cannot silently degrade to singleton
  // matching or cross an owner/provider boundary.
  const baseProviderConnectionId = (connection: InvestecRuntimeConnection): string => {
    const accountId = connection.accountIds[0]
    const suffix = accountId === undefined ? undefined : `:${accountId}`
    return suffix !== undefined && connection.connectionId.endsWith(suffix)
      ? connection.connectionId.slice(0, -suffix.length)
      : connection.connectionId
  }
  const investecGroups = new Map<string, RuntimeConnection[]>()
  for (const connection of connections) {
    if (connection.provider !== 'investec') continue
    const groupKey = JSON.stringify([connection.deploymentId, connection.localOwnerId, baseProviderConnectionId(connection)])
    const group = investecGroups.get(groupKey) ?? []
    group.push(connection)
    investecGroups.set(groupKey, group)
  }
  const completedRuns = new Map(runs.map(item => [item.connection, item]))
  const correlatedInvestec = new Set<RuntimeConnection>()
  const { correlateInvestecTransferObservations, finalizeInvestecObservations } = await import('./probes/investec/investec-adapter')
  for (const expected of investecGroups.values()) {
    const accountIds = new Set(expected.flatMap(connection => connection.accountIds))
    if (expected.length <= 1 || accountIds.size <= 1) continue
    const groupRuns = expected.map(connection => completedRuns.get(connection))
    if (groupRuns.some(item => item === undefined || item.run.transferAccounts === undefined)) {
      // Do not degrade successful siblings to singleton projection: a missing
      // sibling can hide an ambiguity and make an unsafe pair appear unique.
      for (const item of groupRuns) if (item) correlatedInvestec.add(item.connection)
      continue
    }
    const validRuns = groupRuns as Array<{ connection: RuntimeConnection; cursor: string | null; run: ProviderRun }>
    const allObservations = validRuns.flatMap(item => item.run.observations)
    const accounts = Object.assign({}, ...validRuns.map(item => item.run.transferAccounts))
    const correlated = correlateInvestecTransferObservations(allObservations, accounts, baseProviderConnectionId(expected[0] as InvestecRuntimeConnection))
    const byIncarnation = new Map<string, typeof correlated>()
    for (const observation of correlated) {
      const rows = byIncarnation.get(observation.sourceResourceIncarnationId) ?? []
      rows.push(observation)
      byIncarnation.set(observation.sourceResourceIncarnationId, rows)
    }
    validRuns.forEach(item => correlatedInvestec.add(item.connection))
    const prepared = validRuns.map(item => ({
      ...item,
      observations: finalizeInvestecObservations(byIncarnation.get(item.connection.sourceResourceIncarnationId) ?? []),
    }))
    try {
      if (!deps.persistAndEnqueueBatch) throw new Error('atomic grouped persistence is unavailable')
      observations += await deps.persistAndEnqueueBatch(prepared.map(item => ({
        connection: item.connection,
        observations: item.observations,
      })))
    } catch {
      failures += 1
      console.error('Finance Sync connection cycle failed')
      continue
    }
    for (const item of prepared) {
      try {
        if (!controls.projectionEnabled) await deps.saveCheckpoint(item.connection, item.run.nextCursor)
        else {
          const projection = await deps.projectPending(item.connection)
          delivered += projection.delivered; failures += projection.failed
          if (projection.failed === 0) await deps.saveCheckpoint(item.connection, item.run.nextCursor)
        }
      } catch {
        failures += 1
        console.error('Finance Sync connection cycle failed')
      }
    }
  }
  for (const item of runs.filter(value => !correlatedInvestec.has(value.connection))) {
    try {
      observations += await deps.persistAndEnqueue(item.connection, item.run.observations)
      if (!controls.projectionEnabled) await deps.saveCheckpoint(item.connection, item.run.nextCursor)
      else {
        const projection = await deps.projectPending(item.connection)
        delivered += projection.delivered; failures += projection.failed
        if (projection.failed === 0) await deps.saveCheckpoint(item.connection, item.run.nextCursor)
      }
    } catch {
      failures += 1
      console.error('Finance Sync connection cycle failed')
    }
  }
  return { connections: connections.length, observations, delivered, failures }
}
