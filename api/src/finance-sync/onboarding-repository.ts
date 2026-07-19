import { createHash } from 'node:crypto'
import type { FinanceSyncSql } from './storage/pg'

export interface DiscoveredProviderAsset {
  readonly providerAssetId: string
  readonly symbol: string
  readonly name: string
  readonly currency: string
  readonly instrumentType: string
}

export interface ProviderAssetMapping {
  readonly providerAssetId: string
  readonly wealthfolioAssetId: string
}

export interface DiscoveredAccount {
  readonly providerAccountId: string
  readonly name: string
  readonly type: string
  readonly currency: string
  readonly mask?: string
  readonly assets: readonly DiscoveredProviderAsset[]
}

export interface PendingProviderAccount extends DiscoveredAccount {
  readonly connectionId: string
  readonly providerGeneration: number
}

export interface AdmissionMetadata {
  readonly wealthfolioAccountId: string
  readonly providerGeneration: number
  readonly receiverAccessGeneration: number
  readonly baselineStatus: 'pending' | 'verified' | 'failed'
  readonly baselineDigest: string | null
  readonly assetMappingDigest: string
}

function assetMappingDigest(mappings: readonly ProviderAssetMapping[]): string {
  const canonical = [...mappings].sort((a, b) => a.providerAssetId.localeCompare(b.providerAssetId))
  return `sha256:${createHash('sha256').update(JSON.stringify(canonical), 'utf8').digest('hex')}`
}

export class PostgresOnboardingRepository {
  constructor(private readonly sql: FinanceSyncSql, private readonly deploymentId: string) {}

  async listPending(subject: string, provider: string): Promise<readonly PendingProviderAccount[]> {
    return await this.sql<PendingProviderAccount[]>`
      SELECT dpa.connection_id::text AS "connectionId", pc.provider_generation AS "providerGeneration",
        dpa.provider_account_id AS "providerAccountId", dpa.provider_name AS name,
        dpa.provider_type AS type, dpa.currency, dpa.mask, dpa.assets
      FROM discovered_provider_accounts dpa
      JOIN provider_connections pc
        ON pc.deployment_id = dpa.deployment_id AND pc.local_owner_id = dpa.local_owner_id
        AND pc.connection_id = dpa.connection_id
      LEFT JOIN provider_account_admissions paa
        ON paa.deployment_id = dpa.deployment_id AND paa.local_owner_id = dpa.local_owner_id
        AND paa.connection_id = dpa.connection_id AND paa.provider_account_id = dpa.provider_account_id
        AND paa.revoked_at IS NULL
      WHERE dpa.deployment_id = ${this.deploymentId} AND dpa.local_owner_id = ${subject}
        AND pc.provider = ${provider} AND pc.state <> 'disabled'
        AND dpa.discovery_generation = pc.provider_generation
        AND paa.provider_account_id IS NULL
      ORDER BY dpa.provider_name, dpa.provider_account_id
    `
  }

  async admit(input: {
    readonly subject: string; readonly provider: string; readonly connectionId: string
    readonly providerAccountId: string; readonly wealthfolioAccountId: string
    readonly databaseStateDigest: string; readonly providerGeneration: number
    readonly receiverAccessGeneration: number; readonly baselineKind: 'empty' | 'relink'
    readonly priorProviderGeneration?: number; readonly assetMappings: readonly ProviderAssetMapping[]
  }): Promise<AdmissionMetadata> {
    if (!/^sha256:[a-f0-9]{64}$/.test(input.databaseStateDigest)
      || !Number.isSafeInteger(input.providerGeneration) || input.providerGeneration < 1
      || !Number.isSafeInteger(input.receiverAccessGeneration) || input.receiverAccessGeneration < 1
      || (input.baselineKind !== 'empty' && input.baselineKind !== 'relink')
      || (input.baselineKind === 'empty' && input.priorProviderGeneration !== undefined)
      || (input.baselineKind === 'relink' && (!Number.isSafeInteger(input.priorProviderGeneration)
        || input.priorProviderGeneration! < 1 || input.priorProviderGeneration! >= input.providerGeneration))
      || !Array.isArray(input.assetMappings)) throw new Error('baseline proof is invalid')
    const mappingIds = input.assetMappings.map(mapping => mapping.providerAssetId)
    if (new Set(mappingIds).size !== mappingIds.length || input.assetMappings.some(mapping =>
      !mapping.providerAssetId || !mapping.wealthfolioAssetId)) throw new Error('asset mappings are invalid')
    const mappingDigest = assetMappingDigest(input.assetMappings)
    return await this.sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      const existing = await tx<{ wealthfolioAccountId: string; providerGeneration: number; revokedAt: string | null }[]>`
        SELECT paa.wealthfolio_account_id AS "wealthfolioAccountId",
          paa.provider_generation AS "providerGeneration", paa.revoked_at::text AS "revokedAt"
        FROM provider_account_admissions paa
        JOIN provider_connections pc
          ON pc.deployment_id = paa.deployment_id AND pc.local_owner_id = paa.local_owner_id
          AND pc.connection_id = paa.connection_id
        WHERE paa.deployment_id = ${this.deploymentId} AND paa.local_owner_id = ${input.subject}
          AND paa.connection_id = ${input.connectionId}::uuid
          AND paa.provider_account_id = ${input.providerAccountId} AND pc.provider = ${input.provider}
        FOR UPDATE OF paa
      `
      if (input.baselineKind === 'empty' && existing.length !== 0) {
        throw new Error('empty baseline conflicts with existing admission')
      }
      if (input.baselineKind === 'relink' && (existing.length !== 1
        || existing[0]?.wealthfolioAccountId !== input.wealthfolioAccountId
        || existing[0]?.revokedAt === null
        || existing[0]?.providerGeneration !== input.priorProviderGeneration)) {
        throw new Error('relink baseline does not match a revoked admission')
      }
      const admitted = await tx`
        INSERT INTO provider_account_admissions (
          deployment_id, local_owner_id, connection_id, provider_account_id,
          wealthfolio_account_id, provider_generation, receiver_access_generation, asset_mappings, asset_mapping_digest,
          baseline_state, baseline_verified_at, baseline_fingerprint
        )
        SELECT dpa.deployment_id, dpa.local_owner_id, dpa.connection_id, dpa.provider_account_id,
          ${input.wealthfolioAccountId}, ${input.providerGeneration}, ${input.receiverAccessGeneration}, ${tx.json(input.assetMappings as never)},
          ${mappingDigest}, 'verified', now(), ${input.databaseStateDigest}
        FROM discovered_provider_accounts dpa
        JOIN provider_connections pc
          ON pc.deployment_id = dpa.deployment_id AND pc.local_owner_id = dpa.local_owner_id
          AND pc.connection_id = dpa.connection_id
        WHERE dpa.deployment_id = ${this.deploymentId} AND dpa.local_owner_id = ${input.subject}
          AND dpa.connection_id = ${input.connectionId}::uuid
          AND dpa.provider_account_id = ${input.providerAccountId}
          AND pc.provider = ${input.provider} AND pc.state <> 'disabled'
          AND pc.provider_generation = ${input.providerGeneration}
          AND dpa.discovery_generation = pc.provider_generation
          AND (SELECT COALESCE(jsonb_agg(asset->>'providerAssetId' ORDER BY asset->>'providerAssetId'), '[]'::jsonb)
            FROM jsonb_array_elements(dpa.assets) asset) = ${tx.json([...mappingIds].sort() as never)}
        ON CONFLICT (deployment_id, local_owner_id, connection_id, provider_account_id)
        DO UPDATE SET provider_generation = EXCLUDED.provider_generation,
          receiver_access_generation = EXCLUDED.receiver_access_generation,
          asset_mappings = EXCLUDED.asset_mappings,
          asset_mapping_digest = EXCLUDED.asset_mapping_digest,
          baseline_state = 'verified', baseline_verified_at = now(),
          baseline_fingerprint = EXCLUDED.baseline_fingerprint, revoked_at = NULL
        WHERE ${input.baselineKind} = 'relink'
          AND provider_account_admissions.wealthfolio_account_id = EXCLUDED.wealthfolio_account_id
        RETURNING provider_account_id
      `
      const metadata = await this.status(tx, input.subject, input.provider, input.connectionId, input.providerAccountId)
      if (!metadata || metadata.wealthfolioAccountId !== input.wealthfolioAccountId
        || metadata.providerGeneration !== input.providerGeneration
        || metadata.receiverAccessGeneration !== input.receiverAccessGeneration
        || metadata.baselineDigest !== input.databaseStateDigest
        || metadata.assetMappingDigest !== mappingDigest) {
        throw new Error(admitted.length === 0 ? 'admission conflicts with committed mapping' : 'provider account is not discoverable')
      }
      await tx`
        UPDATE provider_connections SET state = 'active'
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${input.connectionId}::uuid AND provider = ${input.provider}
          AND provider_generation = ${input.providerGeneration}
      `
      return metadata
    })
  }

  async reconciliationStatus(subject: string, provider: string, connectionId: string, providerAccountId: string): Promise<AdmissionMetadata | null> {
    return this.status(this.sql, subject, provider, connectionId, providerAccountId)
  }

  private async status(sql: FinanceSyncSql, subject: string, provider: string, connectionId: string, providerAccountId: string): Promise<AdmissionMetadata | null> {
    const rows = await sql<AdmissionMetadata[]>`
      SELECT paa.wealthfolio_account_id AS "wealthfolioAccountId", paa.provider_generation AS "providerGeneration",
        paa.receiver_access_generation AS "receiverAccessGeneration", paa.baseline_state AS "baselineStatus",
        paa.baseline_fingerprint AS "baselineDigest", paa.asset_mapping_digest AS "assetMappingDigest"
      FROM provider_account_admissions paa JOIN provider_connections pc
        ON pc.deployment_id = paa.deployment_id AND pc.local_owner_id = paa.local_owner_id AND pc.connection_id = paa.connection_id
      WHERE paa.deployment_id = ${this.deploymentId} AND paa.local_owner_id = ${subject} AND pc.provider = ${provider}
        AND paa.connection_id = ${connectionId}::uuid AND paa.provider_account_id = ${providerAccountId} AND paa.revoked_at IS NULL
        AND paa.provider_generation = pc.provider_generation
    `
    return rows[0] ?? null
  }

  async recordConnection(input: {
    readonly subject: string
    readonly provider: 'investec' | 'mercury' | 'plaid' | 'snaptrade'
    readonly providerConnectionId: string
    readonly secretReference: string
    readonly configuration?: Readonly<Record<string, unknown>>
    readonly accounts: readonly DiscoveredAccount[]
  }): Promise<void> {
    if (input.accounts.length === 0) throw new Error('provider discovery returned no accounts')
    await this.sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      const principal = await tx<{ provider_principal_id: string }[]>`
        INSERT INTO provider_principals (
          deployment_id, local_owner_id, provider_principal_id, provider, provider_subject_id
        ) VALUES (
          ${this.deploymentId}, ${input.subject}, gen_random_uuid(), ${input.provider}, ${input.providerConnectionId}
        )
        ON CONFLICT (deployment_id, local_owner_id, provider, provider_subject_id)
        DO UPDATE SET provider_subject_id = EXCLUDED.provider_subject_id
        RETURNING provider_principal_id::text
      `
      const principalId = principal[0]?.provider_principal_id
      if (!principalId) throw new Error('provider principal persistence failed')
      const connection = await tx<{ connection_id: string }[]>`
        INSERT INTO provider_connections (
          deployment_id, local_owner_id, provider_principal_id, provider,
          provider_connection_id, secret_reference, configuration, state
        ) VALUES (
          ${this.deploymentId}, ${input.subject}, ${principalId}::uuid, ${input.provider},
          ${input.providerConnectionId}, ${input.secretReference},
          ${tx.json((input.configuration ?? {}) as never)}, 'discovered'
        )
        ON CONFLICT (deployment_id, local_owner_id, provider, provider_connection_id)
        DO UPDATE SET secret_reference = EXCLUDED.secret_reference,
          configuration = EXCLUDED.configuration,
          provider_generation = provider_connections.provider_generation + 1,
          state = 'discovered', disabled_at = NULL
        RETURNING connection_id::text
      `
      const connectionId = connection[0]?.connection_id
      if (!connectionId) throw new Error('provider connection persistence failed')

      // Relink rotates the connection generation. Prior admissions must be
      // explicitly re-established against accounts discovered in that generation.
      await tx`
        UPDATE provider_account_admissions
        SET revoked_at = now(), baseline_state = 'pending', baseline_verified_at = NULL,
          baseline_fingerprint = NULL
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${connectionId}::uuid AND revoked_at IS NULL
      `
      await tx`
        INSERT INTO connection_incarnations (
          deployment_id, local_owner_id, connection_incarnation_id, provider_principal_id,
          provider_connection_id, incarnation
        ) VALUES (
          ${this.deploymentId}, ${input.subject}, ${connectionId}::uuid, ${principalId}::uuid,
          ${input.providerConnectionId}, 1
        ) ON CONFLICT DO NOTHING
      `
      for (const account of input.accounts) {
        const accountRows = await tx<{ account_incarnation_id: string }[]>`
          INSERT INTO account_incarnations (
            deployment_id, local_owner_id, account_incarnation_id, connection_incarnation_id,
            provider_account_id, incarnation
          ) VALUES (
            ${this.deploymentId}, ${input.subject}, gen_random_uuid(), ${connectionId}::uuid,
            ${account.providerAccountId}, 1
          )
          ON CONFLICT (deployment_id, local_owner_id, connection_incarnation_id, provider_account_id, incarnation)
          DO UPDATE SET provider_account_id = EXCLUDED.provider_account_id
          RETURNING account_incarnation_id::text
        `
        const accountIncarnationId = accountRows[0]?.account_incarnation_id
        if (!accountIncarnationId) throw new Error('account incarnation persistence failed')
        const resources = await tx<{ source_resource_incarnation_id: string }[]>`
          INSERT INTO resource_incarnations (
            deployment_id, local_owner_id, source_resource_incarnation_id, account_incarnation_id,
            resource_kind, provider_resource_id, incarnation
          ) VALUES (
            ${this.deploymentId}, ${input.subject}, gen_random_uuid(), ${accountIncarnationId}::uuid,
            'account_projection', ${account.providerAccountId}, 1
          )
          ON CONFLICT (deployment_id, local_owner_id, account_incarnation_id, resource_kind, provider_resource_id, incarnation)
          DO UPDATE SET provider_resource_id = EXCLUDED.provider_resource_id
          RETURNING source_resource_incarnation_id::text
        `
        const resourceId = resources[0]?.source_resource_incarnation_id
        if (!resourceId) throw new Error('resource incarnation persistence failed')
        await tx`
          INSERT INTO discovered_provider_accounts (
            deployment_id, local_owner_id, connection_id, provider_account_id,
            account_incarnation_id, source_resource_incarnation_id, provider_name,
            provider_type, currency, mask, assets, discovery_generation
          ) VALUES (
            ${this.deploymentId}, ${input.subject}, ${connectionId}::uuid, ${account.providerAccountId},
            ${accountIncarnationId}::uuid, ${resourceId}::uuid, ${account.name},
            ${account.type}, ${account.currency}, ${account.mask ?? null}, ${tx.json(account.assets as never)},
            (SELECT provider_generation FROM provider_connections
              WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
                AND connection_id = ${connectionId}::uuid)
          )
          ON CONFLICT (deployment_id, local_owner_id, connection_id, provider_account_id)
          DO UPDATE SET provider_name = EXCLUDED.provider_name, provider_type = EXCLUDED.provider_type,
            currency = EXCLUDED.currency, mask = EXCLUDED.mask, assets = EXCLUDED.assets,
            discovery_generation = EXCLUDED.discovery_generation, discovered_at = now()
        `
      }
    })
  }
}
