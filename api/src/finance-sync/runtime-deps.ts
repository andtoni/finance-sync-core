import type { FinanceSyncSql } from './storage/pg'
import { runPlaidSync, type ObservationOutput, type PlaidTransport } from './probes/plaid/plaid-adapter'
import { cashObservationToBridge, type BridgeDisclosureOperation } from './projection/bridge-client'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'

// G8 resiliency: retry threshold derived from bridge-client.ts commitObservation maxRetries = 3
const MAX_PROJECTION_DELIVERY_ATTEMPTS = 3

/**
 * Structured, redacted audit entry for sync-cycle projection outcomes.
 * Contains only metadata essential for observability — never tokens, payloads,
 * account identifiers, or financial amounts.
 */
export interface SyncCycleAuditEntry {
  readonly deploymentId: string
  readonly connectionId: string
  readonly outcome: 'delivered' | 'retry' | 'dead_letter'
  readonly count: number
}

interface RuntimeEnv { readonly FINANCE_SYNC_CONNECTIONS_JSON?: string; readonly FINANCE_SYNC_BRIDGE_URL?: string; readonly FINANCE_SYNC_PLAID_URL?: string; readonly FINANCE_SYNC_PLAID_CLIENT_ID?: string; readonly FINANCE_SYNC_PLAID_SECRET?: string }
function connections(env: RuntimeEnv, deploymentId: string): readonly RuntimeConnection[] {
  if (!env.FINANCE_SYNC_CONNECTIONS_JSON) return [];
  const value: unknown = JSON.parse(env.FINANCE_SYNC_CONNECTIONS_JSON);
  if (!Array.isArray(value)) throw new Error('FINANCE_SYNC_CONNECTIONS_JSON must be an array');
  return value.map(item => {
    if (!item || typeof item !== 'object') throw new Error('invalid Finance Sync connection');
    const c = item as Record<string, unknown>;
    if (c.provider !== 'plaid') throw new Error(`unsupported runtime provider: ${String(c.provider)}`);
    for (const n of ['connectionId','accessToken','localOwnerId','sourceResourceIncarnationId','deploymentId'])
      if (typeof c[n] !== 'string' || !c[n]) throw new Error(`connection ${n} is required`);
    if (!Array.isArray(c.accountIds) || !c.accountIds.every(x => typeof x === 'string')) throw new Error('connection accountIds is required');
    if (!c.wealthfolioAccountIds || typeof c.wealthfolioAccountIds !== 'object') throw new Error('connection wealthfolioAccountIds is required');
    if (!Number.isSafeInteger(c.accessGeneration) || (c.accessGeneration as number) < 1) throw new Error('connection accessGeneration must be positive');
    // G8 cross-deployment fail-closed: each connection's deploymentId must match the runtime's deploymentId
    if (c.deploymentId !== deploymentId) throw new Error(`connection deploymentId mismatch`);
    return c as unknown as RuntimeConnection
  })
}
function plaidTransport(env: RuntimeEnv): PlaidTransport { const url = env.FINANCE_SYNC_PLAID_URL, clientId = env.FINANCE_SYNC_PLAID_CLIENT_ID, secret = env.FINANCE_SYNC_PLAID_SECRET; if (!url || !clientId || !secret) throw new Error('Plaid endpoint and credentials are required for configured connections'); return { async syncTransactions(access_token, cursor) { const response = await fetch(`${url.replace(/\/$/, '')}/transactions/sync`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ client_id: clientId, secret, access_token, cursor: cursor ?? undefined }) }); if (!response.ok) throw new Error(`Plaid sync failed: HTTP ${response.status}`); return await response.json() as Awaited<ReturnType<PlaidTransport['syncTransactions']>> } } }
export function createRuntimeDependencies(sql: FinanceSyncSql, deploymentId: string, env: RuntimeEnv = process.env): SyncRuntimeDependencies {
  const configured = connections(env, deploymentId)
  // G8 fail-closed startup: validate provider credentials eagerly when connections are configured,
  // before the first sync cycle or DB mutation.
  if (configured.length > 0) {
    plaidTransport(env)
    if (!env.FINANCE_SYNC_BRIDGE_URL) throw new Error('FINANCE_SYNC_BRIDGE_URL is required for configured connections')
  }
  return {
    loadConnections: async () => configured,
    loadCheckpoint: async c => (await sql<{ cursor: string | null }[]>`SELECT cursor FROM sync_checkpoints WHERE deployment_id = ${deploymentId} AND connection_id = ${c.connectionId}`)[0]?.cursor ?? null,
    runProvider: async (c, cursor) => ({ ...await runPlaidSync({ deploymentId, accessToken: c.accessToken, localOwnerId: c.localOwnerId, sourceResourceIncarnationId: c.sourceResourceIncarnationId, accountIds: c.accountIds, initialCursor: cursor ?? undefined }, plaidTransport(env)) }),
    persistObservations: async observations => { const inserted: ObservationOutput[] = []; for (const o of observations) { const rows = await sql`INSERT INTO raw_observations (deployment_id, local_owner_id, observation_id, source_resource_incarnation_id, provider_version, semantic_hash, hash_specification_version, payload, observed_at) VALUES (${o.deploymentId}, ${o.localOwnerId}, gen_random_uuid(), ${o.sourceResourceIncarnationId}::uuid, ${o.providerVersion}, ${o.semanticHash}, ${o.hashSpecificationVersion}, ${sql.json(o.payload as any)}, ${o.observedAt}) ON CONFLICT DO NOTHING RETURNING observation_id`; if (rows.length) inserted.push(o) }; return inserted },
    enqueue: async observations => { for (const o of observations) { const accountId = o.payload.accountId; if (o.domain !== 'cash_ledger' || typeof accountId !== 'string') continue; const c = connections(env, deploymentId).find(x => x.localOwnerId === o.localOwnerId && x.sourceResourceIncarnationId === o.sourceResourceIncarnationId); const account = c?.wealthfolioAccountIds[accountId]; if (!c || !account) continue; const op = cashObservationToBridge(o, c.accessGeneration, account); await sql`INSERT INTO projection_outbox (deployment_id, local_owner_id, connection_id, idempotency_key, payload) VALUES (${o.deploymentId}, ${o.localOwnerId}, ${c.connectionId}, ${op.idempotencyKey}, ${sql.json(op as any)}) ON CONFLICT (deployment_id, idempotency_key) DO NOTHING` } },
    projectPending: async () => {
      if (!env.FINANCE_SYNC_BRIDGE_URL) return { delivered: 0, failed: 0, syncCycleAudit: [] };
      let delivered = 0, failed = 0;
      // Track per-connection-per-outcome counts for structured audit
      const auditMap = new Map<string, { delivered: number; retry: number; dead_letter: number }>();
      function track(connId: string, outcome: 'delivered' | 'retry' | 'dead_letter') {
        let entry = auditMap.get(connId);
        if (!entry) { entry = { delivered: 0, retry: 0, dead_letter: 0 }; auditMap.set(connId, entry); }
        entry[outcome]++
      }
      const rows = await sql<{ id: string; connection_id: string; payload: BridgeDisclosureOperation; attempts: number }[]>`SELECT id, connection_id, payload, attempts FROM projection_outbox WHERE deployment_id = ${deploymentId} AND state = 'pending' ORDER BY created_at FOR UPDATE SKIP LOCKED`;
      for (const row of rows) try {
        const response = await fetch(env.FINANCE_SYNC_BRIDGE_URL, { method: 'POST', headers: { 'content-type': 'application/json', 'idempotency-key': row.payload.idempotencyKey }, body: JSON.stringify(row.payload) });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        await sql`UPDATE projection_outbox SET state = 'delivered', delivered_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = ${row.id}::uuid`;
        delivered++;
        track(row.connection_id, 'delivered')
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        const nextAttempts = row.attempts + 1;
        if (nextAttempts >= MAX_PROJECTION_DELIVERY_ATTEMPTS) {
          await sql`UPDATE projection_outbox SET attempts = ${nextAttempts}, last_error = ${errMsg}, state = 'dead_letter', dead_lettered_at = now(), dead_letter_reason = ${errMsg} WHERE id = ${row.id}::uuid`;
          track(row.connection_id, 'dead_letter')
        } else {
          await sql`UPDATE projection_outbox SET attempts = ${nextAttempts}, last_error = ${errMsg} WHERE id = ${row.id}::uuid`;
          track(row.connection_id, 'retry')
        }
        failed++
      };
      // Build redacted audit entries aggregated per connection per outcome
      const syncCycleAudit: SyncCycleAuditEntry[] = [];
      for (const [connectionId, counts] of auditMap) {
        for (const outcome of ['delivered', 'retry', 'dead_letter'] as const) {
          if (counts[outcome] > 0) syncCycleAudit.push({ deploymentId, connectionId, outcome, count: counts[outcome] })
        }
      }
      return { delivered, failed, syncCycleAudit }
    },
    saveCheckpoint: async (c, cursor) => { await sql`INSERT INTO sync_checkpoints (deployment_id, connection_id, provider, cursor) VALUES (${deploymentId}, ${c.connectionId}, ${c.provider}, ${cursor}) ON CONFLICT (deployment_id, connection_id) DO UPDATE SET cursor = EXCLUDED.cursor, provider = EXCLUDED.provider, updated_at = now()` },
  }
}
