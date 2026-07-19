
import { runPlaidSync, type PlaidTransport, type PlaidSyncConfig } from '../probes/plaid/plaid-adapter'
import { cashObservationToBridge, commitObservationBatch, type BridgeTransport, type BridgeDisclosureOperation } from './bridge-client'

export interface SyncProviderConfig {
  readonly provider: string
  readonly accessToken: string
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly accountIds: readonly string[]
  readonly wealthfolioAccountIds: Record<string, string>
  readonly accessGeneration: number
  readonly initialCursor?: string
  readonly maxPages?: number
}

export interface PlaidSyncSession extends SyncProviderConfig {
  readonly provider: 'plaid'
  readonly transport: PlaidTransport
  readonly bridgeTransport: BridgeTransport
}

export interface SyncSessionResult {
  readonly provider: string
  readonly newCursor: string | null
  readonly observationsCount: number
  readonly committedCount: number
  readonly pagesStaged: number
  readonly errors: readonly string[]
}

/**
 * Run one Plaid sync session end-to-end:
 * provider → observations → bridge operations → Wealthfolio commit.
 */
export async function runPlaidSyncSession(config: PlaidSyncSession): Promise<SyncSessionResult> {
  const errors: string[] = []
  let observationsCount = 0
  let committedCount = 0

  try {
    const syncResult = await runPlaidSync({
      accessToken: config.accessToken,
      deploymentId: config.deploymentId,
      localOwnerId: config.localOwnerId,
      sourceResourceIncarnationId: config.sourceResourceIncarnationId,
      accountIds: config.accountIds,
      initialCursor: config.initialCursor,
      maxPages: config.maxPages,
    }, config.transport)

    observationsCount = syncResult.observations.length

    // Build bridge operations from observations
    const operations: BridgeDisclosureOperation[] = []
    for (const obs of syncResult.observations) {
      // This legacy session projects transaction activity only. Complete balance
      // facts are handled atomically by the durable current-state assembler.
      if (!obs.providerVersion.startsWith('plaid:txn:') && obs.payload.isRemoved !== true) continue
      // Map provider account ID to Wealthfolio account ID
      const p = obs.payload as Record<string, unknown>
      const providerAccountId = p.accountId as string
      const wfAccountId = config.wealthfolioAccountIds[providerAccountId]
      if (!wfAccountId) {
        errors.push(`No Wealthfolio mapping for account: ${providerAccountId}`)
        continue
      }

      const op = cashObservationToBridge(obs, config.accessGeneration, wfAccountId)
      operations.push(op)
    }

    // Commit all operations
    if (operations.length > 0) {
      const receipts = await commitObservationBatch(config.bridgeTransport, operations)
      committedCount = receipts.filter(r => r.outcome === 'Accepted').length
    }

    return {
      provider: config.provider,
      newCursor: syncResult.nextCursor,
      observationsCount,
      committedCount,
      pagesStaged: syncResult.pagesStaged,
      errors,
    }
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error)
    errors.push(msg)
    return {
      provider: config.provider,
      newCursor: config.initialCursor ?? null,
      observationsCount,
      committedCount,
      pagesStaged: 0,
      errors,
    }
  }
}
