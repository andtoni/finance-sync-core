import type { FinanceSyncSql } from './storage/pg'
import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import {
  createProviderTransportFactories,
  parseRuntimeConnections,
  runRuntimeProvider,
  type ProviderRuntimeEnv,
  type ProviderTransportFactories,
} from './provider-runtime'
import {
  assertIssuerQualifiedSubject,
  bridgeOperationToWire,
  providerObservationToBridge,
  validateBridgeReceipt,
  type BridgeDisclosureOperation,
} from './projection/bridge-client'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'

const MAX_PROJECTION_DELIVERY_ATTEMPTS = 3
const MAX_PROJECTION_BATCH = 25
const PLAID_HOSTS = new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com'])

export interface SyncCycleAuditEntry {
  readonly deploymentId: string
  readonly connectionId: string
  readonly outcome: 'delivered' | 'retry' | 'dead_letter'
  readonly count: number
}

interface RuntimeEnv extends ProviderRuntimeEnv {
  readonly FINANCE_SYNC_BRIDGE_URL?: string
  readonly FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS?: string
  readonly FINANCE_SYNC_BRIDGE_TOKEN?: string
}

function requiredEndpoint(raw: string | undefined, name: string, allowedHosts: ReadonlySet<string>): URL {
  if (!raw) throw new Error(`${name} is required for configured connections`)
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    throw new Error(`${name} must be a valid URL`)
  }
  if (url.protocol !== 'https:' || url.username || url.password) throw new Error(`${name} must be an HTTPS URL without credentials`)
  if (!allowedHosts.has(url.hostname.toLowerCase())) throw new Error(`${name} host is not allowed`)
  return url
}

function bridgeHosts(env: RuntimeEnv): ReadonlySet<string> {
  const hosts = env.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS?.split(',').map(value => value.trim().toLowerCase()).filter(Boolean) ?? []
  if (hosts.length === 0) throw new Error('FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS is required for configured connections')
  return new Set(hosts)
}

function safeDeliveryError(error: unknown): string {
  if (error instanceof Error && /^bridge (delivery failed: HTTP \d{3}|receipt)/.test(error.message)) return error.message.slice(0, 80)
  return 'bridge delivery failed'
}

export function createRuntimeDependencies(
  sql: FinanceSyncSql,
  deploymentId: string,
  env: RuntimeEnv = process.env,
  transportFactories?: ProviderTransportFactories,
): SyncRuntimeDependencies {
  const configured = parseRuntimeConnections(env, deploymentId)
  const factories = transportFactories ?? createProviderTransportFactories(env)
  let bridgeUrl: URL | undefined
  if (configured.length > 0) {
    // Construct every configured provider transport at startup so endpoint and
    // server-credential errors fail before the service reports readiness.
    for (const connection of configured) {
      if (connection.provider === 'plaid') factories.plaid(connection)
      else if (connection.provider === 'investec') factories.investec(connection)
      else if (connection.provider === 'mercury') factories.mercury(connection)
      else factories.snaptrade(connection)
    }
    bridgeUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_URL, 'FINANCE_SYNC_BRIDGE_URL', bridgeHosts(env))
    if (!env.FINANCE_SYNC_BRIDGE_TOKEN) throw new Error('FINANCE_SYNC_BRIDGE_TOKEN is required for configured connections')
  }

  return {
    loadConnections: async () => configured,
    loadCheckpoint: async connection => (
      await sql<{ cursor: string | null }[]>`
        SELECT cursor FROM sync_checkpoints
        WHERE deployment_id = ${deploymentId} AND connection_id = ${connection.connectionId}
      `
    )[0]?.cursor ?? null,
    runProvider: async (connection, cursor) => runRuntimeProvider(connection, cursor, factories),
    persistAndEnqueue: async (connection, observations) => sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      let insertedCount = 0
      for (const observation of observations) {
        if (observation.deploymentId !== deploymentId
          || observation.localOwnerId !== connection.localOwnerId
          || observation.sourceResourceIncarnationId !== connection.sourceResourceIncarnationId) {
          throw new Error('observation authority mismatch')
        }
        let accountId = typeof observation.payload.accountId === 'string' ? observation.payload.accountId : undefined
        if (observation.payload.isRemoved === true && !accountId) {
          const transactionId = observation.payload.transactionId
          if (typeof transactionId !== 'string' || !transactionId) throw new Error('removed transaction identity is required')
          const prior = await tx<{ account_id: string }[]>`
            SELECT payload->>'accountId' AS account_id
            FROM raw_observations
            WHERE deployment_id = ${deploymentId}
              AND local_owner_id = ${connection.localOwnerId}
              AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
              AND provider_version = ${`${connection.provider}:txn:${transactionId}`}
              AND payload ? 'accountId'
            ORDER BY recorded_at DESC
            LIMIT 1
          `
          accountId = prior[0]?.account_id
        }
        if (!accountId || !connection.accountIds.includes(accountId)) throw new Error('provider account is outside the admitted connection scope')
        const wealthfolioAccountId = connection.wealthfolioAccountIds[accountId]
        if (!wealthfolioAccountId) throw new Error('provider account has no Wealthfolio mapping')
        const enriched: ObservationOutput = observation.payload.accountId === accountId
          ? observation
          : { ...observation, payload: Object.freeze({ ...observation.payload, accountId }) }
        const inserted = await tx`
          INSERT INTO raw_observations (
            deployment_id, local_owner_id, observation_id, source_resource_incarnation_id,
            provider_version, semantic_hash, hash_specification_version, payload, observed_at
          ) VALUES (
            ${enriched.deploymentId}, ${enriched.localOwnerId}, gen_random_uuid(),
            ${enriched.sourceResourceIncarnationId}::uuid, ${enriched.providerVersion},
            ${enriched.semanticHash}, ${enriched.hashSpecificationVersion},
            ${tx.json(enriched.payload as never)}, ${enriched.observedAt}
          )
          ON CONFLICT DO NOTHING
          RETURNING observation_id
        `
        insertedCount += inserted.length
        const operation = providerObservationToBridge(connection.provider, enriched, connection.accessGeneration, wealthfolioAccountId)
        if (operation) await tx`
          INSERT INTO projection_outbox (
            deployment_id, local_owner_id, connection_id, idempotency_key, payload
          ) VALUES (
            ${deploymentId}, ${connection.localOwnerId}, ${connection.connectionId},
            ${operation.idempotencyKey}, ${tx.json(operation as never)}
          )
          ON CONFLICT (deployment_id, idempotency_key) DO NOTHING
        `
      }
      return insertedCount
    }),
    projectPending: async connection => {
      if (!bridgeUrl || !env.FINANCE_SYNC_BRIDGE_TOKEN) return { delivered: 0, failed: 0 }
      return sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        const unresolvedBefore = await tx<{ count: number }[]>`
          SELECT count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND state = 'dead_letter'
        `
        if ((unresolvedBefore[0]?.count ?? 0) > 0) return { delivered: 0, failed: unresolvedBefore[0].count }

        let delivered = 0
        let deliveryFailures = 0
        const rows = await tx<{ id: string; payload: BridgeDisclosureOperation; attempts: number }[]>`
          SELECT id, payload, attempts
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND state = 'pending'
          ORDER BY created_at
          LIMIT ${MAX_PROJECTION_BATCH}
          FOR UPDATE SKIP LOCKED
        `
        for (const row of rows) {
          try {
            const response = await fetch(bridgeUrl, {
              method: 'POST',
              redirect: 'error',
              headers: {
                'content-type': 'application/json',
                authorization: `Bearer ${env.FINANCE_SYNC_BRIDGE_TOKEN}`,
                'idempotency-key': row.payload.idempotencyKey,
              },
              body: JSON.stringify(bridgeOperationToWire(row.payload)),
            })
            if (!response.ok) throw new Error(`bridge delivery failed: HTTP ${response.status}`)
            validateBridgeReceipt(row.payload, await response.json())
            await tx`
              UPDATE projection_outbox
              SET state = 'delivered', delivered_at = now(), attempts = attempts + 1, last_error = NULL
              WHERE id = ${row.id}::uuid
            `
            delivered++
          } catch (error) {
            const reason = safeDeliveryError(error)
            const nextAttempts = row.attempts + 1
            if (nextAttempts >= MAX_PROJECTION_DELIVERY_ATTEMPTS) {
              await tx`
                UPDATE projection_outbox
                SET attempts = ${nextAttempts}, last_error = ${reason}, state = 'dead_letter',
                    dead_lettered_at = now(), dead_letter_reason = ${reason}
                WHERE id = ${row.id}::uuid
              `
            } else {
              await tx`
                UPDATE projection_outbox SET attempts = ${nextAttempts}, last_error = ${reason}
                WHERE id = ${row.id}::uuid
              `
            }
            deliveryFailures++
          }
        }
        const unresolvedAfter = await tx<{ count: number }[]>`
          SELECT count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND state IN ('pending', 'dead_letter')
        `
        return { delivered, failed: Math.max(deliveryFailures, unresolvedAfter[0]?.count ?? 0) }
      })
    },
    saveCheckpoint: async (connection, cursor) => {
      await sql`
        INSERT INTO sync_checkpoints (deployment_id, connection_id, provider, cursor)
        VALUES (${deploymentId}, ${connection.connectionId}, ${connection.provider}, ${cursor})
        ON CONFLICT (deployment_id, connection_id) DO UPDATE
        SET cursor = EXCLUDED.cursor, provider = EXCLUDED.provider, updated_at = now()
      `
    },
  }
}
