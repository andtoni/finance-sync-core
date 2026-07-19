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
  currentStateToBridge,
  providerObservationToBridge,
  validateBridgeReceipt,
  type BridgeDisclosureOperation,
} from './projection/bridge-client'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import { readExclusiveInput } from './secret-input'
import { loadDurableRuntimeConnections, type ProviderSecretResolver } from './durable-connections'
import { unavailableProviderSecretStore } from './connect-control'
import { assembleCurrentState } from './projection/current-state'

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
  readonly FINANCE_SYNC_BRIDGE_RECEIPT_URL?: string
  readonly FINANCE_SYNC_BRIDGE_TOKEN?: string
  readonly FINANCE_SYNC_BRIDGE_TOKEN_FILE?: string
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

const UUID_PREFIX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:/i

/**
 * Durable connections use `<provider-connection-uuid>:<provider-account-id>`.
 * Non-matching IDs are retained only for isolated legacy unit paths; they can
 * never be returned by the PostgreSQL loader.
 */
async function assertDurableAuthority(sql: FinanceSyncSql, deploymentId: string, connection: RuntimeConnection): Promise<void> {
  if (!UUID_PREFIX.test(connection.connectionId)) return
  const separator = connection.connectionId.indexOf(':')
  const connectionId = connection.connectionId.slice(0, separator)
  const providerAccountId = connection.connectionId.slice(separator + 1)
  if (!providerAccountId || connection.accountIds.length !== 1 || connection.accountIds[0] !== providerAccountId) {
    throw new Error('durable connection identity is invalid')
  }
  const authorized = await sql<{ authorized: number }[]>`
    SELECT 1 AS authorized
    FROM provider_connections pc
    JOIN discovered_provider_accounts dpa
      ON dpa.deployment_id = pc.deployment_id AND dpa.local_owner_id = pc.local_owner_id
      AND dpa.connection_id = pc.connection_id
    JOIN provider_account_admissions paa
      ON paa.deployment_id = dpa.deployment_id AND paa.local_owner_id = dpa.local_owner_id
      AND paa.connection_id = dpa.connection_id AND paa.provider_account_id = dpa.provider_account_id
    WHERE pc.deployment_id = ${deploymentId} AND pc.local_owner_id = ${connection.localOwnerId}
      AND pc.connection_id = ${connectionId}::uuid AND pc.provider = ${connection.provider}
      AND pc.state = 'active' AND pc.disabled_at IS NULL
      AND pc.provider_generation = ${connection.providerGeneration}
      AND dpa.provider_account_id = ${providerAccountId}
      AND dpa.source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
      AND dpa.discovery_generation = pc.provider_generation
      AND paa.revoked_at IS NULL AND paa.baseline_state = 'verified'
      AND paa.receiver_access_generation = ${connection.receiverAccessGeneration}
      AND paa.wealthfolio_account_id = ${connection.wealthfolioAccountIds[providerAccountId]}
    FOR SHARE OF pc, dpa, paa
  `
  if (authorized.length !== 1) throw new Error('durable connection authority is stale')
}

export type BridgeReceiptLookup = (operation: BridgeDisclosureOperation) => Promise<unknown | null>

export function createRuntimeDependencies(
  sql: FinanceSyncSql,
  deploymentId: string,
  env: RuntimeEnv = process.env,
  transportFactories?: ProviderTransportFactories,
  receiptLookup?: BridgeReceiptLookup,
  secretResolver: ProviderSecretResolver = unavailableProviderSecretStore,
): SyncRuntimeDependencies {
  // Legacy metadata is parsed only as a startup validation aid for existing
  // deployments/tests. It is never returned by loadConnections; PostgreSQL is
  // the sole runtime admission authority.
  const legacyValidationOnly = parseRuntimeConnections(env, deploymentId)
  const factories = transportFactories ?? createProviderTransportFactories(env)
  for (const connection of legacyValidationOnly) {
    if (connection.provider === 'plaid') factories.plaid(connection)
    else if (connection.provider === 'investec') factories.investec(connection)
    else if (connection.provider === 'mercury') factories.mercury(connection)
    else factories.snaptrade(connection)
  }
  let bridgeUrl: URL | undefined
  let bridgeReceiptUrl: URL | undefined
  let bridgeToken: string | undefined
  const bridgeInputs = [env.FINANCE_SYNC_BRIDGE_URL, env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, env.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS,
    env.FINANCE_SYNC_BRIDGE_TOKEN, env.FINANCE_SYNC_BRIDGE_TOKEN_FILE]
  if (bridgeInputs.some(Boolean)) {
    const allowedBridgeHosts = bridgeHosts(env)
    bridgeUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_URL, 'FINANCE_SYNC_BRIDGE_URL', allowedBridgeHosts)
    bridgeReceiptUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, 'FINANCE_SYNC_BRIDGE_RECEIPT_URL', allowedBridgeHosts)
    bridgeToken = readExclusiveInput(env as Readonly<Record<string, string | undefined>>, 'FINANCE_SYNC_BRIDGE_TOKEN', true)
  }

  return {
    loadConnections: async () => {
      const connections = await loadDurableRuntimeConnections(sql, deploymentId, secretResolver)
      if (connections.length > 0 && (!bridgeUrl || !bridgeReceiptUrl || !bridgeToken)) throw new Error('bridge configuration is required for admitted connections')
      return connections
    },
    loadCheckpoint: async connection => {
      await assertDurableAuthority(sql, deploymentId, connection)
      return (await sql<{ cursor: string | null }[]>`
        SELECT cursor FROM sync_checkpoints
        WHERE deployment_id = ${deploymentId} AND connection_id = ${connection.connectionId}
      `)[0]?.cursor ?? null
    },
    runProvider: async (connection, cursor) => runRuntimeProvider(connection, cursor, factories),
    persistAndEnqueue: async (connection, observations) => sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      await assertDurableAuthority(tx, deploymentId, connection)
      const currentStates = assembleCurrentState(connection.provider, observations, connection.accountIds.map(providerAccountId => ({
        providerAccountId,
        accountId: connection.wealthfolioAccountIds[providerAccountId],
      })), {
        providerConnectionId: connection.connectionId,
        providerIncarnationId: connection.sourceResourceIncarnationId,
        providerGeneration: connection.providerGeneration,
      })
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
        const operation = providerObservationToBridge(connection.provider, enriched, connection.receiverAccessGeneration, wealthfolioAccountId, connection.connectionId)
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
      for (const current of currentStates) {
        const operation = currentStateToBridge({
          deploymentId,
          localOwnerId: connection.localOwnerId,
          sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
        }, current.payload, current.semanticHash, connection.receiverAccessGeneration, connection.connectionId)
        await tx`
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
      if (!bridgeUrl || !bridgeToken) return { delivered: 0, failed: 0 }
      return sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        await assertDurableAuthority(tx, deploymentId, connection)
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
        const rows = await tx<{ id: string; payload: BridgeDisclosureOperation; attempts: number; state: 'pending' | 'uncertain' }[]>`
          SELECT id, payload, attempts, state
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND state IN ('pending', 'uncertain')
          ORDER BY created_at
          LIMIT ${MAX_PROJECTION_BATCH}
          FOR UPDATE SKIP LOCKED
        `
        for (const row of rows) {
          const markDelivered = async () => {
            await tx`
              UPDATE projection_outbox
              SET state = 'delivered', delivered_at = now(), attempts = attempts + 1,
                  last_error = NULL, dead_lettered_at = NULL, dead_letter_reason = NULL
              WHERE id = ${row.id}::uuid
            `
            delivered++
          }
          const recoverReceipt = async (): Promise<boolean> => {
            try {
              const lookup = receiptLookup ?? (async (operation: BridgeDisclosureOperation): Promise<unknown | null> => {
                if (!bridgeReceiptUrl || !bridgeToken) return null
                const base = bridgeReceiptUrl.toString().replace(/\/+$/, '')
                const response = await fetch(`${base}/${encodeURIComponent(operation.subject)}/${encodeURIComponent(operation.idempotencyKey)}`, {
                  method: 'GET', redirect: 'error', headers: { authorization: `Bearer ${bridgeToken}` },
                })
                if (response.status === 404) return null
                if (!response.ok) throw new Error('bridge receipt lookup failed')
                return response.json()
              })
              const recovered = await lookup(row.payload)
              if (recovered === null) return false
              validateBridgeReceipt(row.payload, recovered)
              await markDelivered()
              return true
            } catch {
              return false
            }
          }

          // Never replay an uncertain commit before attempting receipt recovery.
          if (row.state === 'uncertain' && await recoverReceipt()) continue

          let response: Response
          try {
            response = await fetch(bridgeUrl, {
              method: 'POST',
              redirect: 'error',
              headers: {
                'content-type': 'application/json',
                authorization: `Bearer ${bridgeToken}`,
                'idempotency-key': row.payload.idempotencyKey,
              },
              body: JSON.stringify(bridgeOperationToWire(row.payload)),
            })
          } catch {
            if (await recoverReceipt()) continue
            await tx`
              UPDATE projection_outbox
              SET attempts = attempts + 1, last_error = 'BRIDGE_RESPONSE_UNCERTAIN', state = 'uncertain'
              WHERE id = ${row.id}::uuid
            `
            deliveryFailures++
            continue
          }

          if (!response.ok) {
            const nextAttempts = row.attempts + 1
            if (nextAttempts >= MAX_PROJECTION_DELIVERY_ATTEMPTS) {
              await tx`
                UPDATE projection_outbox
                SET attempts = ${nextAttempts}, last_error = 'BRIDGE_HTTP_REJECTED', state = 'dead_letter',
                    dead_lettered_at = now(), dead_letter_reason = 'BRIDGE_HTTP_REJECTED'
                WHERE id = ${row.id}::uuid
              `
            } else {
              await tx`
                UPDATE projection_outbox SET attempts = ${nextAttempts}, last_error = 'BRIDGE_HTTP_REJECTED'
                WHERE id = ${row.id}::uuid
              `
            }
            deliveryFailures++
            continue
          }

          try {
            validateBridgeReceipt(row.payload, await response.json())
            await markDelivered()
          } catch {
            if (await recoverReceipt()) continue
            // A successful HTTP commit with a lost/invalid receipt is uncertain,
            // not terminal. It can only be resolved by a bound receipt lookup.
            await tx`
              UPDATE projection_outbox
              SET attempts = attempts + 1, last_error = 'BRIDGE_RECEIPT_UNCERTAIN', state = 'uncertain'
              WHERE id = ${row.id}::uuid
            `
            deliveryFailures++
          }
        }
        const unresolvedAfter = await tx<{ count: number }[]>`
          SELECT count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND state IN ('pending', 'uncertain', 'dead_letter')
        `
        return { delivered, failed: Math.max(deliveryFailures, unresolvedAfter[0]?.count ?? 0) }
      })
    },
    saveCheckpoint: async (connection, cursor) => sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      await assertDurableAuthority(tx, deploymentId, connection)
      await tx`
        INSERT INTO sync_checkpoints (deployment_id, connection_id, provider, cursor)
        VALUES (${deploymentId}, ${connection.connectionId}, ${connection.provider}, ${cursor})
        ON CONFLICT (deployment_id, connection_id) DO UPDATE
        SET cursor = EXCLUDED.cursor, provider = EXCLUDED.provider, updated_at = now()
      `
    }),
  }
}
