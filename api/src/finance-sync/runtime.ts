import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import { getProviderEntry, type ProviderName } from './probes/provider-registry'
import { PLAID_ITEM_ERROR_CODES } from './provider-runtime'

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
  /** Bounded Plaid ingestion counters. Counts only — never provider text or values. */
  readonly plaidSync?: PlaidSyncDiagnostics
  /** Provider accounts for which this run carries no balance fact. Never logged. */
  readonly currentStateUnavailableAccountIds?: readonly string[]
  /** Admitted provider accounts the provider did not return; the run carries nothing for them. Never logged. */
  readonly accountsNotReturned?: readonly string[]
}

/**
 * Bounded Plaid ingestion counters used to make silent row filtering visible.
 * `skipped*` counts explain why a provider row never became an observation.
 */
export interface PlaidSyncDiagnostics {
  readonly added: number
  readonly modified: number
  readonly removed: number
  readonly skippedUnadmittedAccount: number
  readonly skippedMissingAccountMetadata: number
  readonly skippedMissingCurrency: number
  /** Accounts whose current-state snapshot was skipped for lack of a provider balance. */
  readonly skippedMissingBalance: number
  /** Admitted accounts the provider did not return; nothing was ingested for them. */
  readonly skippedAccountNotReturned: number
}
export interface PersistEnqueueOptions { readonly skipCurrentState?: boolean }
export interface TerminalRevisionResolver {
  resolve(operation: import('./projection/bridge-client').BridgeDisclosureOperation): Promise<{
    readonly lifecycleOperation: import('./projection/bridge-client').BridgeDisclosureOperation
    readonly ordinaryOperation: import('./projection/bridge-client').BridgeDisclosureOperation
    readonly boundReceipt: unknown
  }>
  deliver(operation: import('./projection/bridge-client').BridgeDisclosureOperation): Promise<unknown>
}
export interface SyncRuntimeDependencies {
  readonly terminalRevisionResolver?: TerminalRevisionResolver
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
/**
 * A condition of one Plaid Item that only its owner can resolve. It is reported
 * per Item and is never a service failure. `reason` is a documented Plaid
 * ITEM_ERROR code or the fixed internal code ACCOUNT_NOT_RETURNED; never
 * provider text. It is absent when Plaid sent no code or an undocumented one.
 */
export interface PlaidItemCondition {
  readonly providerConnectionId: string
  readonly reason?: string
}
export interface SyncCycleResult {
  readonly connections: number; readonly observations: number; readonly delivered: number; readonly failures: number
  /** Plaid Items that need their owner's attention this cycle; they do not count as failures. */
  readonly plaidItemConditions: readonly PlaidItemCondition[]
}
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

/** Bounded Plaid ingestion counters. Logged only when the cycle saw rows or dropped rows. */
function logPlaidItemObserved(run: ProviderRun): void {
  const counts = run.plaidSync
  if (!counts) return
  const total = counts.added + counts.modified + counts.removed
    + counts.skippedUnadmittedAccount + counts.skippedMissingAccountMetadata + counts.skippedMissingCurrency
    + counts.skippedMissingBalance + counts.skippedAccountNotReturned
  if (total === 0) return
  console.log('FINANCE_SYNC_PLAID_ITEM_OBSERVED', JSON.stringify(counts))
}

/**
 * Finance Sync's own fixed internal failure codes. These are compile-time
 * constants, not provider text, and are the only messages ever emitted.
 */
const INTERNAL_FAILURE_CODES = new Set([
  'INCOMPLETE_ACCOUNT_BALANCE_CURRENCY',
  'MISSING_CURSOR_AFTER_HAS_MORE',
  'PAGE_BOUND_EXCEEDED',
  'MUTATION_RESTART_EXHAUSTED',
  'Plaid item has no admitted accounts',
  'Plaid item authority generations are ambiguous',
  'Plaid item providerConnectionId is required',
  'Plaid item providerConnectionId mismatch',
  'Plaid item secretReference mismatch',
  'Plaid item clientId mismatch',
  'Plaid item clientSecret mismatch',
  'Plaid item accessToken mismatch',
  'Plaid item deploymentId mismatch',
  'Plaid item localOwnerId mismatch',
  'Plaid item accountIds overlap',
  'Plaid item account mapping is incomplete',
  'Plaid item wealthfolioAccountIds conflict',
  'Plaid item source resource mapping is incomplete',
  'durable connection authority is stale',
  'durable connection identity is invalid',
  'plaid item authority is stale',
  'Plaid item provider connection identity is required',
  'provider account is outside the admitted connection scope',
  'provider account has no Wealthfolio mapping',
])

/**
 * Safe provider-failure diagnostic. Emits only a fixed stage, the JavaScript
 * error class, an allowlisted Plaid error code, and — when the failure is one
 * of Finance Sync's own fixed internal codes — that code. Provider text,
 * identifiers, tokens, and financial values are never emitted.
 */
type PlaidFailureStage =
  | 'item_checkpoint' | 'item_build' | 'item_sync' | 'item_cursor' | 'persist_or_project'

/** Tags the failing phase without wrapping or mutating the underlying error. */
class PlaidStageFailure extends Error {
  constructor(readonly stage: PlaidFailureStage, readonly failure: unknown) { super(stage) }
}

function runPlaidStage<T>(stage: PlaidFailureStage, operation: () => T | Promise<T>): Promise<T> {
  return Promise.resolve().then(operation).catch((error: unknown) => {
    throw new PlaidStageFailure(stage, error)
  })
}

/**
 * ITEM_ERROR codes that are not a condition of the Item's ownership or access:
 * PRODUCT_NOT_READY means Plaid is still preparing data and clears by itself,
 * so asking the owner to reconnect would be wrong. It stays a cycle failure.
 */
const PLAID_TRANSIENT_ITEM_ERROR_CODES = new Set(['PRODUCT_NOT_READY'])

/**
 * An ITEM_ERROR answer to the Item's own provider request is a condition of
 * that one Item, not of the service. Every other error type, every other
 * stage, and anything unclassified stays a cycle failure.
 */
function plaidItemCondition(error: unknown): { readonly reason?: string } | undefined {
  if (!(error instanceof PlaidStageFailure) || error.stage !== 'item_sync') return undefined
  const failure = error.failure
  if (typeof failure !== 'object' || failure === null) return undefined
  const { code, errorType } = failure as { code?: unknown; errorType?: unknown }
  if (errorType !== 'ITEM_ERROR') return undefined
  if (typeof code !== 'string' || !PLAID_ITEM_ERROR_CODES.has(code)) return {}
  return PLAID_TRANSIENT_ITEM_ERROR_CODES.has(code) ? undefined : { reason: code }
}

/** The fixed reason code only; no identifier of the Item or its accounts. */
function logPlaidItemNeedsAttention(reason: string | undefined): void {
  console.error('FINANCE_SYNC_PLAID_ITEM_NEEDS_ATTENTION', JSON.stringify(reason === undefined ? {} : { reason }))
}

function logPlaidItemFailure(stage: PlaidFailureStage, error: unknown): void {
  const failure = error instanceof PlaidStageFailure ? error.failure : error
  const resolvedStage = error instanceof PlaidStageFailure ? error.stage : stage
  const code = typeof failure === 'object' && failure !== null && 'code' in failure
    && typeof (failure as { code?: unknown }).code === 'string'
    ? (failure as { code: string }).code
    : undefined
  const errorType = typeof failure === 'object' && failure !== null && 'errorType' in failure
    && typeof (failure as { errorType?: unknown }).errorType === 'string'
    ? (failure as { errorType: string }).errorType
    : undefined
  const message = failure instanceof Error ? failure.message : undefined
  const detail = message !== undefined && INTERNAL_FAILURE_CODES.has(message) ? message : undefined
  console.error('FINANCE_SYNC_PLAID_ITEM_FAILED', JSON.stringify({
    stage: resolvedStage,
    errorClass: failure instanceof Error ? failure.name : typeof failure,
    ...(code === undefined ? {} : { code }),
    ...(errorType === undefined ? {} : { errorType }),
    ...(detail === undefined ? {} : { detail }),
  }))
}

/** Read-only provider ingestion. With projection enabled, cursor promotion waits for full outbox delivery. */
export async function runConfiguredSyncCycle(
  deploymentId: string,
  deps: SyncRuntimeDependencies,
  controls: SyncCycleControls = enabledByDefault,
): Promise<SyncCycleResult> {
  if (!controls.providersEnabled) return { connections: 0, observations: 0, delivered: 0, failures: 0, plaidItemConditions: [] }
  const connections = await deps.loadConnections(deploymentId)
  let observations = 0; let delivered = 0; let failures = 0
  const plaidItemConditions: PlaidItemCondition[] = []
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
        const item = await runPlaidStage('item_build', () => plaidItemConnection(expected))
        const cursor = await runPlaidStage('item_checkpoint', () => deps.loadPlaidItemCheckpoint!(item))
        const run = await runPlaidStage('item_sync', () => deps.runProvider(item, cursor))
        logPlaidItemObserved(run)
        const bySourceResource = new Map<string, ObservationOutput[]>()
        for (const observation of run.observations) {
          const rows = bySourceResource.get(observation.sourceResourceIncarnationId) ?? []
          rows.push(observation)
          bySourceResource.set(observation.sourceResourceIncarnationId, rows)
        }
        let itemFailed = false
        const withoutBalance = new Set(run.currentStateUnavailableAccountIds ?? [])
        const notReturned = new Set(run.accountsNotReturned ?? [])
        for (const connection of expected) {
          // Nothing is written for an account the provider did not return.
          if (connection.accountIds.some(accountId => notReturned.has(accountId))) continue
          const accountObservations = connection.accountIds.flatMap(accountId =>
            bySourceResource.get(connection.sourceResourceIncarnationIds?.[accountId] ?? connection.sourceResourceIncarnationId) ?? [])
          // Without a provider balance no snapshot is assembled for this
          // connection at all, so the complete-snapshot rule is never asked to
          // accept a partial one and the receiver keeps its last full snapshot.
          const skipCurrentState = connection.accountIds.some(accountId => withoutBalance.has(accountId))
          try {
            observations += await runPlaidStage('persist_or_project', () => skipCurrentState
              ? deps.persistAndEnqueue(connection, accountObservations, { skipCurrentState: true })
              : deps.persistAndEnqueue(connection, accountObservations))
            if (!controls.projectionEnabled) continue
            const projection = await runPlaidStage('persist_or_project', () => deps.projectPending(connection))
            delivered += projection.delivered
            failures += projection.failed
            if (projection.failed > 0) itemFailed = true
          } catch (error) {
            itemFailed = true
            failures += 1
            logPlaidItemFailure('persist_or_project', error)
            console.error('Finance Sync connection cycle failed')
          }
        }
        if (notReturned.size > 0) {
          plaidItemConditions.push({ providerConnectionId: connectionGroupId(item), reason: 'ACCOUNT_NOT_RETURNED' })
          logPlaidItemNeedsAttention('ACCOUNT_NOT_RETURNED')
        }
        if (itemFailed) {
          failures += 1
          console.error('Finance Sync Plaid item cursor held')
        } else if (notReturned.size === 0) {
          // With an account missing the item-level cursor stays where it is:
          // advancing it would drop whatever the stream holds for that account.
          // The returned accounts are re-read from the same cursor and converge
          // idempotently.
          await runPlaidStage('item_cursor', () => deps.savePlaidItemCheckpoint!(item, run.nextCursor))
          console.log('FINANCE_SYNC_PLAID_ITEM_SYNC')
        }
      } catch (error) {
        logPlaidItemFailure('item_sync', error)
        const providerConnectionId = expected[0]?.providerConnectionId
        const condition = plaidItemCondition(error)
        if (providerConnectionId && condition) {
          // Nothing was ingested and the cursor is untouched; only the owner can
          // restore this Item, so it must not hold the service unhealthy.
          plaidItemConditions.push({ providerConnectionId, ...condition })
          logPlaidItemNeedsAttention(condition.reason)
          continue
        }
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
  return { connections: connections.length, observations, delivered, failures, plaidItemConditions }
}
