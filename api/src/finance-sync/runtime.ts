import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import { getProviderEntry, type ProviderName } from './probes/provider-registry'

interface RuntimeConnectionBase {
  readonly provider: ProviderName
  readonly connectionId: string
  /** Stable provider-connection identity shared by all admitted accounts. */
  readonly providerConnectionId?: string
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
  /** Per-account native resource identity retained while one Plaid item is synced once. */
  readonly sourceResourceIncarnationIds?: Readonly<Record<string, string>>
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
  loadPlaidItemCheckpoint?(connection: Extract<RuntimeConnection, { provider: 'plaid' }>): Promise<string | null>
  runProvider(connection: RuntimeConnection, cursor: string | null): Promise<ProviderRun>
  persistAndEnqueue(connection: RuntimeConnection, observations: readonly ObservationOutput[], options?: PersistEnqueueOptions): Promise<number>
  persistAndEnqueueBatch?(batches: readonly {
    readonly connection: RuntimeConnection
    readonly observations: readonly ObservationOutput[]
    readonly options?: PersistEnqueueOptions
  }[]): Promise<number>
  projectPending(connection: RuntimeConnection): Promise<{ delivered: number; failed: number }>
  saveCheckpoint(connection: RuntimeConnection, cursor: string | null): Promise<void>
  savePlaidItemCheckpoint?(connection: Extract<RuntimeConnection, { provider: 'plaid' }>, cursor: string | null): Promise<void>
}
export interface SyncCycleResult { readonly connections: number; readonly observations: number; readonly delivered: number; readonly failures: number }
export interface SyncCycleControls { readonly providersEnabled: boolean; readonly projectionEnabled: boolean }

const enabledByDefault: SyncCycleControls = Object.freeze({ providersEnabled: true, projectionEnabled: true })

function connectionPrefix(connection: RuntimeConnection): string {
  const accountId = connection.accountIds[0]
  const suffix = accountId === undefined ? undefined : `:${accountId}`
  return suffix !== undefined && connection.connectionId.endsWith(suffix)
    ? connection.connectionId.slice(0, -suffix.length)
    : connection.connectionId
}

function connectionGroupId(connection: RuntimeConnection): string {
  return connection.providerConnectionId ?? connectionPrefix(connection)
}

function plaidItemConnection(connections: readonly PlaidRuntimeConnection[]): PlaidRuntimeConnection {
  const first = connections[0]
  if (!first) throw new Error('Plaid item has no admitted accounts')
  const generations = new Set(connections.map(connection => connection.providerGeneration))
  const accessGenerations = new Set(connections.map(connection => connection.receiverAccessGeneration))
  if (generations.size !== 1 || accessGenerations.size !== 1) throw new Error('Plaid item authority generations are ambiguous')
  // Fail closed on conflicting item identity and credentials
  const providerConnectionId = first.providerConnectionId
  if (!providerConnectionId) throw new Error('Plaid item providerConnectionId is required')
  for (const connection of connections) {
    if (connection.providerConnectionId !== providerConnectionId) throw new Error('Plaid item providerConnectionId mismatch')
    if (connection.secretReference !== first.secretReference) throw new Error('Plaid item secretReference mismatch')
    if (connection.clientId !== first.clientId) throw new Error('Plaid item clientId mismatch')
    if (connection.clientSecret !== first.clientSecret) throw new Error('Plaid item clientSecret mismatch')
    if (connection.accessToken !== first.accessToken) throw new Error('Plaid item accessToken mismatch')
    if (connection.deploymentId !== first.deploymentId) throw new Error('Plaid item deploymentId mismatch')
    if (connection.localOwnerId !== first.localOwnerId) throw new Error('Plaid item localOwnerId mismatch')
  }
  // Detect duplicate/overlapping account IDs
  const accountSeen = new Map<string, string>()
  const accountToWealthfolio = new Map<string, string>()
  for (const connection of connections) {
    for (const accountId of connection.accountIds) {
      if (accountSeen.has(accountId)) throw new Error('Plaid item accountIds overlap')
      accountSeen.set(accountId, connection.connectionId)
      const wf = connection.wealthfolioAccountIds[accountId]
      if (!wf) throw new Error('Plaid item account mapping is incomplete')
      if (accountToWealthfolio.has(accountId) && accountToWealthfolio.get(accountId) !== wf) {
        throw new Error('Plaid item wealthfolioAccountIds conflict')
      }
      accountToWealthfolio.set(accountId, wf)
    }
  }
  const accountIds = [...accountSeen.keys()].sort()
  const wealthfolioAccountIds = Object.fromEntries(accountIds.map(accountId => [accountId, accountToWealthfolio.get(accountId)!]))
  const sourceResourceIncarnationIds = Object.fromEntries(accountIds.map(accountId => {
    const owner = connections.find(connection => connection.sourceResourceIncarnationIds?.[accountId] !== undefined
      || connection.accountIds.includes(accountId))
    const source = owner?.sourceResourceIncarnationIds?.[accountId] ?? owner?.sourceResourceIncarnationId
    if (!source) throw new Error('Plaid item source resource mapping is incomplete')
    return [accountId, source]
  }))
  return {
    ...first,
    connectionId: connectionPrefix(first),
    accountIds,
    wealthfolioAccountIds,
    sourceResourceIncarnationIds,
  }
}

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
  const plaidGroups = new Map<string, PlaidRuntimeConnection[]>()
  for (const connection of connections) {
    if (connection.provider !== 'plaid') continue
    const key = JSON.stringify([connection.deploymentId, connection.localOwnerId, connectionGroupId(connection)])
    const group = plaidGroups.get(key) ?? []
    group.push(connection)
    plaidGroups.set(key, group)
  }
  const plaidItemCursorEnabled = typeof deps.loadPlaidItemCheckpoint === 'function'
    && typeof deps.savePlaidItemCheckpoint === 'function'
  const plaidItemConnections = new Set<RuntimeConnection>(plaidItemCursorEnabled
    ? [...plaidGroups.values()].flat()
    : [])
  for (const connection of connections) {
    getProviderEntry(connection.provider)
    if (connection.provider === 'plaid' && plaidItemConnections.has(connection)) continue
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
  // Plaid Transactions Sync cursors are item-level. Keep the existing
  // per-account runtime connections for source identities, persistence, and
  // projection, but perform one provider request per item and commit its
  // cursor only after every admitted account has projected successfully.
  if (plaidItemCursorEnabled) {
    for (const expected of plaidGroups.values()) {
      if (expected.length === 0) continue
      try {
        const item = plaidItemConnection(expected)
        const cursor = await deps.loadPlaidItemCheckpoint!(item)
        const run = await deps.runProvider(item, cursor)
        const bySourceResource = new Map<string, ObservationOutput[]>()
        for (const observation of run.observations) {
          const rows = bySourceResource.get(observation.sourceResourceIncarnationId) ?? []
          rows.push(observation)
          bySourceResource.set(observation.sourceResourceIncarnationId, rows)
        }
        let itemFailed = false
        for (const connection of expected) {
          const accountObservations = connection.accountIds.flatMap(accountId =>
            bySourceResource.get(connection.sourceResourceIncarnationIds?.[accountId] ?? connection.sourceResourceIncarnationId) ?? [])
          try {
            observations += await deps.persistAndEnqueue(connection, accountObservations)
            if (!controls.projectionEnabled) continue
            const projection = await deps.projectPending(connection)
            delivered += projection.delivered
            failures += projection.failed
            if (projection.failed > 0) itemFailed = true
          } catch {
            itemFailed = true
            failures += 1
            console.error('Finance Sync connection cycle failed')
          }
        }
        if (itemFailed) {
          failures += 1
          console.error('Finance Sync Plaid item cursor held')
        } else {
          await deps.savePlaidItemCheckpoint!(item, run.nextCursor)
          console.log('FINANCE_SYNC_PLAID_ITEM_SYNC')
        }
      } catch {
        failures += 1
        console.error('Finance Sync connection cycle failed')
      }
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
