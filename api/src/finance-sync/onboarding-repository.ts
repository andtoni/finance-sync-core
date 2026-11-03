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

export interface MovedAdmission {
  readonly wealthfolioAccountId: string
  readonly connectionId: string
  readonly providerAccountId: string
  readonly providerGeneration: number
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
        -- The same bank account can be visible to more than one Item of one
        -- institution. Offering it again cannot succeed: the account already
        -- exists in Wealthfolio, and a relink is only monotonic within the
        -- Item that owns it. Hide offers whose mask is already connected
        -- through a different Item so the panel stops proposing them.
        AND NOT (
          dpa.mask IS NOT NULL AND dpa.mask <> ''
          AND EXISTS (
            SELECT 1 FROM provider_account_admissions other
            JOIN discovered_provider_accounts other_accounts
              ON other_accounts.deployment_id = other.deployment_id
              AND other_accounts.local_owner_id = other.local_owner_id
              AND other_accounts.connection_id = other.connection_id
              AND other_accounts.provider_account_id = other.provider_account_id
            WHERE other.deployment_id = dpa.deployment_id
              AND other.local_owner_id = dpa.local_owner_id
              AND other.revoked_at IS NULL
              AND other.connection_id <> dpa.connection_id
              AND other_accounts.mask = dpa.mask
          )
        )
      ORDER BY dpa.provider_name, dpa.provider_account_id
    `
  }

  /**
   * Names and masks of the accounts the runtime actually syncs: the same
   * active, current-generation, baseline-verified predicate as the durable
   * runtime loader. Used only to label a connection for the owner.
   */
  async listConnectedAccounts(subject: string, provider: string): Promise<readonly {
    readonly providerConnectionId: string; readonly name: string; readonly mask: string | null
  }[]> {
    return await this.sql<{ providerConnectionId: string; name: string; mask: string | null }[]>`
      SELECT pc.provider_connection_id AS "providerConnectionId", dpa.provider_name AS name, dpa.mask
      FROM provider_connections pc
      JOIN discovered_provider_accounts dpa
        ON dpa.deployment_id = pc.deployment_id AND dpa.local_owner_id = pc.local_owner_id
        AND dpa.connection_id = pc.connection_id
      JOIN provider_account_admissions paa
        ON paa.deployment_id = dpa.deployment_id AND paa.local_owner_id = dpa.local_owner_id
        AND paa.connection_id = dpa.connection_id AND paa.provider_account_id = dpa.provider_account_id
      WHERE pc.deployment_id = ${this.deploymentId} AND pc.local_owner_id = ${subject}
        AND pc.provider = ${provider} AND pc.state = 'active' AND pc.disabled_at IS NULL
        AND dpa.discovery_generation = pc.provider_generation
        AND paa.provider_generation = pc.provider_generation
        AND paa.revoked_at IS NULL AND paa.baseline_state = 'verified'
      ORDER BY pc.provider_connection_id, dpa.provider_name, dpa.provider_account_id
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
      await tx`
        UPDATE provider_connections SET state = 'active'
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${input.connectionId}::uuid AND provider = ${input.provider}
          AND provider_generation = ${input.providerGeneration}
      `
      // Wealthfolio access generation is deployment-wide, not account-local. Granting
      // another account advances it for every still-authorized account, so retain one
      // generation across all active admissions for this subject.
      await tx`
        UPDATE provider_account_admissions paa
        SET receiver_access_generation = ${input.receiverAccessGeneration}
        FROM provider_connections pc
        WHERE paa.deployment_id = ${this.deploymentId} AND paa.local_owner_id = ${input.subject}
          AND paa.revoked_at IS NULL AND paa.receiver_access_generation < ${input.receiverAccessGeneration}
          AND pc.deployment_id = paa.deployment_id AND pc.local_owner_id = paa.local_owner_id
          AND pc.connection_id = paa.connection_id AND pc.disabled_at IS NULL
          AND pc.provider_generation = paa.provider_generation
      `
      // Rebind only unresolved operations for accounts that remain actively admitted.
      // Their idempotency keys do not include access generation, and no prior receipt
      // exists, so the payload can safely retry at the newly granted generation.
      await tx`
        UPDATE projection_outbox po
        SET payload = jsonb_set(po.payload, '{accessGeneration}', to_jsonb(${input.receiverAccessGeneration}::bigint), false),
          state = 'pending', attempts = 0, last_error = NULL,
          dead_lettered_at = NULL, dead_letter_reason = NULL
        WHERE po.deployment_id = ${this.deploymentId} AND po.local_owner_id = ${input.subject}
          AND po.state IN ('pending', 'uncertain', 'dead_letter')
          AND (po.payload->>'accessGeneration')::bigint < ${input.receiverAccessGeneration}
          AND (po.state <> 'dead_letter' OR po.dead_letter_reason = 'BRIDGE_HTTP_REJECTED')
          AND EXISTS (
            SELECT 1 FROM provider_account_admissions active
            JOIN provider_connections pc
              ON pc.deployment_id = active.deployment_id AND pc.local_owner_id = active.local_owner_id
              AND pc.connection_id = active.connection_id
            WHERE active.deployment_id = po.deployment_id AND active.local_owner_id = po.local_owner_id
              AND (active.connection_id::text = po.connection_id
                OR po.connection_id LIKE active.connection_id::text || ':%')
              AND active.wealthfolio_account_id = po.payload->>'accountId'
              AND active.revoked_at IS NULL AND pc.disabled_at IS NULL
              AND active.provider_generation = po.provider_generation
              AND pc.provider_generation = active.provider_generation
          )
      `
      const metadata = await this.status(tx, input.subject, input.provider, input.connectionId, input.providerAccountId)
      if (!metadata || metadata.wealthfolioAccountId !== input.wealthfolioAccountId
        || metadata.providerGeneration !== input.providerGeneration
        || metadata.receiverAccessGeneration !== input.receiverAccessGeneration
        || metadata.baselineDigest !== input.databaseStateDigest
        || metadata.assetMappingDigest !== mappingDigest) {
        throw new Error(admitted.length === 0 ? 'admission conflicts with committed mapping' : 'provider account is not discoverable')
      }
      return metadata
    })
  }

  /** Local revocation is generation-fenced and never calls a provider write API. */
  async revoke(subject: string, provider: string, connectionId: string): Promise<boolean> {
    return await this.sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      const connection = await tx<{ connectionId: string }[]>`
        UPDATE provider_connections
        SET provider_generation = provider_generation + 1, state = 'disabled', disabled_at = now()
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${subject}
          AND provider = ${provider} AND connection_id = ${connectionId}::uuid AND state <> 'disabled'
        RETURNING connection_id::text AS "connectionId"
      `
      if (connection.length !== 1) return false
      await tx`
        UPDATE provider_account_admissions SET revoked_at = now(), baseline_state = 'pending',
          baseline_verified_at = NULL, baseline_fingerprint = NULL
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${subject}
          AND connection_id = ${connection[0]!.connectionId}::uuid AND revoked_at IS NULL
      `
      return true
    })
  }

  /**
   * Moves the active Plaid admission of one Wealthfolio account to the provider
   * account its institution re-issued it as, in the same connection or in
   * another one. The two discovered accounts exchange their source resources,
   * so the admission keeps its observations, their identities and the activity
   * ids already delivered, and the account left behind holds none. Repeating a
   * completed move returns the same result.
   */
  async moveAdmission(input: {
    readonly subject: string; readonly wealthfolioAccountId: string
    readonly connectionId: string; readonly providerAccountId: string
    /** Where the last sync cycle saw this admission returned as the requested account; absent otherwise. */
    readonly from?: { readonly connectionId: string; readonly providerAccountId: string }
  }): Promise<MovedAdmission> {
    return await this.sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      const [source] = await tx<{
        connectionId: string; providerAccountId: string; providerGeneration: number; receiverAccessGeneration: number
        assetMappings: readonly ProviderAssetMapping[]; assetMappingDigest: string; baselineFingerprint: string
        accountIncarnationId: string; sourceResourceIncarnationId: string
        mask: string | null; name: string; currency: string
      }[]>`
        SELECT paa.connection_id::text AS "connectionId", paa.provider_account_id AS "providerAccountId",
          pc.provider_generation AS "providerGeneration", paa.receiver_access_generation AS "receiverAccessGeneration",
          paa.asset_mappings AS "assetMappings", paa.asset_mapping_digest AS "assetMappingDigest",
          paa.baseline_fingerprint AS "baselineFingerprint",
          dpa.account_incarnation_id::text AS "accountIncarnationId",
          dpa.source_resource_incarnation_id::text AS "sourceResourceIncarnationId",
          dpa.mask, dpa.provider_name AS name, dpa.currency
        FROM provider_account_admissions paa
        JOIN provider_connections pc
          ON pc.deployment_id = paa.deployment_id AND pc.local_owner_id = paa.local_owner_id
          AND pc.connection_id = paa.connection_id
        JOIN discovered_provider_accounts dpa
          ON dpa.deployment_id = paa.deployment_id AND dpa.local_owner_id = paa.local_owner_id
          AND dpa.connection_id = paa.connection_id AND dpa.provider_account_id = paa.provider_account_id
        WHERE paa.deployment_id = ${this.deploymentId} AND paa.local_owner_id = ${input.subject}
          AND paa.wealthfolio_account_id = ${input.wealthfolioAccountId}
          AND pc.provider = 'plaid' AND pc.state = 'active' AND pc.disabled_at IS NULL
          AND dpa.discovery_generation = pc.provider_generation
          AND paa.provider_generation = pc.provider_generation
          AND paa.revoked_at IS NULL AND paa.baseline_state = 'verified'
        FOR UPDATE OF paa, dpa
      `
      if (!source) throw new Error('admission to move was not found')
      if (source.connectionId === input.connectionId && source.providerAccountId === input.providerAccountId) {
        return {
          wealthfolioAccountId: input.wealthfolioAccountId, connectionId: source.connectionId,
          providerAccountId: source.providerAccountId, providerGeneration: source.providerGeneration,
        }
      }
      if (input.from?.connectionId !== source.connectionId || input.from.providerAccountId !== source.providerAccountId) {
        throw new Error('admission is not returned under the requested provider account')
      }
      // The re-issued account must be discovered in a connected Item and carry
      // the identity the sync cycle recognised it by.
      const [target] = await tx<{ providerGeneration: number; accountIncarnationId: string; sourceResourceIncarnationId: string }[]>`
        SELECT pc.provider_generation AS "providerGeneration",
          dpa.account_incarnation_id::text AS "accountIncarnationId",
          dpa.source_resource_incarnation_id::text AS "sourceResourceIncarnationId"
        FROM discovered_provider_accounts dpa
        JOIN provider_connections pc
          ON pc.deployment_id = dpa.deployment_id AND pc.local_owner_id = dpa.local_owner_id
          AND pc.connection_id = dpa.connection_id
        WHERE dpa.deployment_id = ${this.deploymentId} AND dpa.local_owner_id = ${input.subject}
          AND dpa.connection_id = ${input.connectionId}::uuid
          AND dpa.provider_account_id = ${input.providerAccountId}
          AND pc.provider = 'plaid' AND pc.state = 'active' AND pc.disabled_at IS NULL
          AND dpa.discovery_generation = pc.provider_generation
          AND dpa.mask = ${source.mask} AND dpa.provider_name = ${source.name} AND dpa.currency = ${source.currency}
        FOR UPDATE OF dpa
      `
      if (!target || !source.mask) throw new Error('re-issued provider account is not discovered')
      const earlier = await tx<{ wealthfolioAccountId: string; revokedAt: string | null }[]>`
        SELECT wealthfolio_account_id AS "wealthfolioAccountId", revoked_at::text AS "revokedAt"
        FROM provider_account_admissions
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${input.connectionId}::uuid AND provider_account_id = ${input.providerAccountId}
        FOR UPDATE
      `
      if (earlier[0]?.revokedAt === null) throw new Error('re-issued provider account belongs to another admission')
      // A revoked admission of the re-issued account to another Wealthfolio
      // account was abandoned: it gives no authority, and its row is replaced
      // below. What it observed stays with its own source resource, which the
      // exchange hands to the account left behind, so none of it can reach the
      // Wealthfolio account that is kept. Observations with no such admission
      // to explain them, or taken for this same Wealthfolio account, would
      // split one history over two source resources and are refused.
      const abandoned = earlier[0] !== undefined && earlier[0].wealthfolioAccountId !== input.wealthfolioAccountId
      const held = abandoned ? [] : await tx`
        SELECT 1 FROM raw_observations
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND source_resource_incarnation_id = ${target.sourceResourceIncarnationId}::uuid
        LIMIT 1
      `
      if (held.length !== 0) throw new Error('re-issued provider account already holds observations')
      // Projections queued under the previous runtime connection could no
      // longer be delivered or recovered once the admission has left it. The
      // runtime connection moved to must be clear as well: anything an
      // abandoned admission left queued there would be delivered under the
      // new admission's authority.
      const unresolved = await tx`
        SELECT 1 FROM projection_outbox
        WHERE deployment_id = ${this.deploymentId}
          AND connection_id IN (${`${source.connectionId}:${source.providerAccountId}`}, ${`${input.connectionId}:${input.providerAccountId}`})
          AND state IN ('pending', 'uncertain', 'dead_letter')
        LIMIT 1
      `
      if (unresolved.length !== 0) throw new Error('admission has undelivered projections')
      await tx`
        UPDATE discovered_provider_accounts
        SET account_incarnation_id = ${target.accountIncarnationId}::uuid,
          source_resource_incarnation_id = ${target.sourceResourceIncarnationId}::uuid
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${source.connectionId}::uuid AND provider_account_id = ${source.providerAccountId}
      `
      await tx`
        UPDATE discovered_provider_accounts
        SET account_incarnation_id = ${source.accountIncarnationId}::uuid,
          source_resource_incarnation_id = ${source.sourceResourceIncarnationId}::uuid
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${input.connectionId}::uuid AND provider_account_id = ${input.providerAccountId}
      `
      // One Wealthfolio account has one active admission, so the previous one
      // is revoked before the new one exists.
      await tx`
        UPDATE provider_account_admissions SET revoked_at = now(), baseline_state = 'pending',
          baseline_verified_at = NULL, baseline_fingerprint = NULL
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND connection_id = ${source.connectionId}::uuid AND provider_account_id = ${source.providerAccountId}
      `
      await tx`
        INSERT INTO provider_account_admissions (
          deployment_id, local_owner_id, connection_id, provider_account_id,
          wealthfolio_account_id, provider_generation, receiver_access_generation, asset_mappings, asset_mapping_digest,
          baseline_state, baseline_verified_at, baseline_fingerprint
        ) VALUES (
          ${this.deploymentId}, ${input.subject}, ${input.connectionId}::uuid, ${input.providerAccountId},
          ${input.wealthfolioAccountId}, ${target.providerGeneration}, ${source.receiverAccessGeneration},
          ${tx.json(source.assetMappings as never)}, ${source.assetMappingDigest},
          'verified', now(), ${source.baselineFingerprint}
        )
        ON CONFLICT (deployment_id, local_owner_id, connection_id, provider_account_id)
        DO UPDATE SET wealthfolio_account_id = EXCLUDED.wealthfolio_account_id, admitted_at = now(),
          provider_generation = EXCLUDED.provider_generation,
          receiver_access_generation = EXCLUDED.receiver_access_generation,
          asset_mappings = EXCLUDED.asset_mappings,
          asset_mapping_digest = EXCLUDED.asset_mapping_digest,
          baseline_state = 'verified', baseline_verified_at = now(),
          baseline_fingerprint = EXCLUDED.baseline_fingerprint, revoked_at = NULL
      `
      return {
        wealthfolioAccountId: input.wealthfolioAccountId, connectionId: input.connectionId,
        providerAccountId: input.providerAccountId, providerGeneration: target.providerGeneration,
      }
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

  private async upsertDiscoveredAccounts(tx: FinanceSyncSql, input: {
    readonly subject: string
    readonly connectionId: string
    readonly providerGeneration: number
    readonly accounts: readonly DiscoveredAccount[]
  }): Promise<void> {
    for (const account of input.accounts) {
      const accountRows = await tx<{ account_incarnation_id: string }[]>`
        INSERT INTO account_incarnations (
          deployment_id, local_owner_id, account_incarnation_id, connection_incarnation_id,
          provider_account_id, incarnation
        ) VALUES (
          ${this.deploymentId}, ${input.subject}, gen_random_uuid(), ${input.connectionId}::uuid,
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
          ${this.deploymentId}, ${input.subject}, ${input.connectionId}::uuid, ${account.providerAccountId},
          ${accountIncarnationId}::uuid, ${resourceId}::uuid, ${account.name},
          ${account.type}, ${account.currency}, ${account.mask ?? null}, ${tx.json(account.assets as never)},
          ${input.providerGeneration}
        )
        ON CONFLICT (deployment_id, local_owner_id, connection_id, provider_account_id)
        DO UPDATE SET provider_name = EXCLUDED.provider_name, provider_type = EXCLUDED.provider_type,
          currency = EXCLUDED.currency, mask = EXCLUDED.mask, assets = EXCLUDED.assets,
          discovery_generation = EXCLUDED.discovery_generation, discovered_at = now()
      `
    }
  }

  async updateConnectionAccounts(input: {
    readonly subject: string
    readonly providerConnectionId: string
    readonly secretReference: string
    readonly providerGeneration: number
    readonly accounts: readonly DiscoveredAccount[]
  }): Promise<void> {
    if (input.accounts.length === 0) throw new Error('provider discovery returned no accounts')
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(input.secretReference)) throw new Error('invalid provider secret reference')
    await this.sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      await tx`SELECT pg_advisory_xact_lock(hashtext(${this.deploymentId}), hashtext(${input.secretReference}))`
      const connections = await tx<{ connectionId: string; principalId: string; providerGeneration: number }[]>`
        SELECT connection_id::text AS "connectionId", provider_principal_id::text AS "principalId",
          provider_generation AS "providerGeneration"
        FROM provider_connections
        WHERE deployment_id = ${this.deploymentId}
          AND local_owner_id = ${input.subject}
          AND provider = 'plaid'
          AND provider_connection_id = ${input.providerConnectionId}
          AND secret_reference = ${input.secretReference}
          AND provider_generation = ${input.providerGeneration}
          AND state = 'active' AND disabled_at IS NULL
        FOR UPDATE
      `
      const connection = connections[0]
      if (!connection || connections.length !== 1) throw new Error('active Plaid connection was not found')
      await tx`
        INSERT INTO connection_incarnations (
          deployment_id, local_owner_id, connection_incarnation_id, provider_principal_id,
          provider_connection_id, incarnation
        ) VALUES (
          ${this.deploymentId}, ${input.subject}, ${connection.connectionId}::uuid, ${connection.principalId}::uuid,
          ${input.providerConnectionId}, 1
        ) ON CONFLICT DO NOTHING
      `
      await this.upsertDiscoveredAccounts(tx, {
        subject: input.subject,
        connectionId: connection.connectionId,
        providerGeneration: connection.providerGeneration,
        accounts: input.accounts,
      })
    })
  }

  async recordConnection(input: {
    readonly subject: string
    readonly provider: 'investec' | 'mercury' | 'plaid' | 'snaptrade'
    readonly providerConnectionId: string
    readonly secretReference: string
    readonly recovery?: boolean
    readonly configuration?: Readonly<Record<string, unknown>>
    readonly accounts: readonly DiscoveredAccount[]
  }): Promise<void> {
    if (input.accounts.length === 0) throw new Error('provider discovery returned no accounts')
    await this.sql.begin(async transaction => this.recordConnectionTransaction(transaction as unknown as FinanceSyncSql, input))
  }

  async recordConnectionTransaction(tx: FinanceSyncSql, input: {
    readonly subject: string
    readonly provider: 'investec' | 'mercury' | 'plaid' | 'snaptrade'
    readonly providerConnectionId: string
    readonly secretReference: string
    readonly recovery?: boolean
    readonly configuration?: Readonly<Record<string, unknown>>
    readonly accounts: readonly DiscoveredAccount[]
  }): Promise<void> {
      if (input.provider === 'plaid') {
        // Match Connect slot-allocation locking so an exact slot cannot be
        // concurrently assigned across subjects while connection state advances.
        await tx`SELECT pg_advisory_xact_lock(hashtext(${this.deploymentId}), hashtext(${input.secretReference}))`
      }
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
      const prior = await tx<{ provider_connection_id: string; secret_reference: string; provider_generation: number; state: string }[]>`
        SELECT provider_connection_id, secret_reference, provider_generation, state
        FROM provider_connections
        WHERE deployment_id = ${this.deploymentId} AND local_owner_id = ${input.subject}
          AND provider = ${input.provider} AND provider_connection_id = ${input.providerConnectionId}
        FOR UPDATE
      `
      if (input.recovery === true && prior[0] && prior[0].secret_reference !== input.secretReference) {
        throw new Error('recovery secret reference does not match the persisted Plaid Item')
      }
      const sameRecovery = input.recovery === true && prior[0]?.provider_connection_id === input.providerConnectionId
        && prior[0]?.secret_reference === input.secretReference && prior[0]?.state === 'active'
      const connection = await tx<{ connection_id: string; provider_generation: number }[]>`
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
          provider_generation = CASE WHEN ${input.recovery ?? false} THEN provider_connections.provider_generation ELSE provider_connections.provider_generation + 1 END,
          state = CASE WHEN ${sameRecovery} THEN provider_connections.state ELSE 'discovered' END,
          disabled_at = CASE WHEN ${sameRecovery} THEN provider_connections.disabled_at ELSE NULL END
        RETURNING connection_id::text, provider_generation
      `
      const connectionId = connection[0]?.connection_id
      const providerGeneration = connection[0]?.provider_generation
      if (!connectionId || providerGeneration === undefined) throw new Error('provider connection persistence failed')

      // Relink rotates the connection generation. Recovery of the same Item,
      // reference, and generation keeps its already-admitted accounts live.
      if (!sameRecovery) await tx`
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
      await this.upsertDiscoveredAccounts(tx, {
        subject: input.subject,
        connectionId,
        providerGeneration,
        accounts: input.accounts,
      })
  }
}
