import type { RuntimeConnection } from './runtime'
import { parseRuntimeConnections } from './provider-runtime'
import type { FinanceSyncSql } from './storage/pg'

export interface ProviderSecretResolver {
  resolve(reference: string): Promise<Readonly<Record<string, unknown>>>
}

interface RuntimeRow {
  connection_id: string
  local_owner_id: string
  provider: RuntimeConnection['provider']
  provider_connection_id: string
  secret_reference: string
  provider_generation: number
  receiver_access_generation: number
  configuration: Record<string, unknown>
  provider_account_id: string
  wealthfolio_account_id: string
  asset_mappings: readonly { providerAssetId: string; wealthfolioAssetId: string }[]
  source_resource_incarnation_id: string
}

/**
 * Loads only active, admitted, baseline-verified accounts. One runtime entry is
 * emitted per admitted account so an account cannot gain projection authority
 * through another account's cursor or mapping.
 */
export async function loadDurableRuntimeConnections(
  sql: FinanceSyncSql,
  deploymentId: string,
  secrets: ProviderSecretResolver,
): Promise<readonly RuntimeConnection[]> {
  const rows = await sql<RuntimeRow[]>`
    SELECT pc.connection_id::text, pc.local_owner_id, pc.provider,
      pc.provider_connection_id, pc.secret_reference, pc.provider_generation,
      pc.configuration, dpa.provider_account_id, paa.wealthfolio_account_id,
      paa.receiver_access_generation, paa.asset_mappings,
      dpa.source_resource_incarnation_id::text
    FROM provider_connections pc
    JOIN discovered_provider_accounts dpa
      ON dpa.deployment_id = pc.deployment_id
      AND dpa.local_owner_id = pc.local_owner_id
      AND dpa.connection_id = pc.connection_id
    JOIN provider_account_admissions paa
      ON paa.deployment_id = dpa.deployment_id
      AND paa.local_owner_id = dpa.local_owner_id
      AND paa.connection_id = dpa.connection_id
      AND paa.provider_account_id = dpa.provider_account_id
    WHERE pc.deployment_id = ${deploymentId}
      AND pc.state = 'active'
      AND pc.disabled_at IS NULL
      AND dpa.discovery_generation = pc.provider_generation
      AND paa.provider_generation = pc.provider_generation
      AND paa.revoked_at IS NULL
      AND paa.baseline_state = 'verified'
    ORDER BY pc.connection_id, dpa.provider_account_id
  `

  const result: RuntimeConnection[] = []
  for (const row of rows) {
    const secret = await secrets.resolve(row.secret_reference)
    const runtimeConnectionId = `${row.connection_id}:${row.provider_account_id}`
    const metadata = {
      ...row.configuration,
      provider: row.provider,
      connectionId: runtimeConnectionId,
      deploymentId,
      localOwnerId: row.local_owner_id,
      sourceResourceIncarnationId: row.source_resource_incarnation_id,
      accountIds: [row.provider_account_id],
      wealthfolioAccountIds: { [row.provider_account_id]: row.wealthfolio_account_id },
      providerGeneration: row.provider_generation,
      receiverAccessGeneration: row.receiver_access_generation,
      ...(row.provider === 'snaptrade' ? { assetMappings: Object.fromEntries(row.asset_mappings.map(mapping =>
        [mapping.providerAssetId, mapping.wealthfolioAssetId])) } : {}),
    }
    const [connection] = parseRuntimeConnections({
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([metadata]),
      FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify({ [runtimeConnectionId]: secret }),
    }, deploymentId)
    result.push(connection)
  }
  return result
}
