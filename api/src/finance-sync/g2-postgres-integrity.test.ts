import { describe, expect, it, beforeAll, afterAll } from 'vitest'
import postgres from 'postgres'
import { migrate } from './storage/migrate'
import { resolve } from 'node:path'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
let _sql: postgres.Sql<Record<string, never>>
function sql() { return _sql }
const deploymentId = 'g2-pg-test'
const adminUrl = url?.replace('/finance_sync_test', '')

suite('G2 PostgreSQL integrity', () => {
  beforeAll(async () => {
    // Reset database for clean state
    const admin = postgres(adminUrl! + '/template1', { connect_timeout: 5, prepare: false })
    await admin`DROP DATABASE IF EXISTS finance_sync_test`
    await admin`CREATE DATABASE finance_sync_test`
    await admin.end()

    const pg = postgres(url!, { connect_timeout: 5, prepare: false })
    const directory = resolve(import.meta.dirname ?? process.cwd(), '../../finance-sync-migrations')
    process.env.FINANCE_SYNC_DEPLOYMENT_ID = deploymentId
    await migrate(pg, directory)
    await pg`DELETE FROM finance_sync_migrations WHERE deployment_id = ${deploymentId}`
    _sql = pg

    // Seed test data
    await sql()`
      INSERT INTO provider_principals (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
      VALUES (${deploymentId}, 'owner-a', 'plaid', '00000000-0000-0000-0000-000000000001', 'plaid-user-1')
    `
    await sql()`
      INSERT INTO connection_incarnations (deployment_id, local_owner_id, connection_incarnation_id, provider_principal_id, provider_connection_id, incarnation)
      VALUES (${deploymentId}, 'owner-a', '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'conn-1', 1)
    `
    await sql()`
      INSERT INTO account_incarnations (deployment_id, local_owner_id, account_incarnation_id, connection_incarnation_id, provider_account_id, incarnation)
      VALUES (${deploymentId}, 'owner-a', '00000000-0000-0000-0000-000000000020', '00000000-0000-0000-0000-000000000010', 'acct-1', 1)
    `
    await sql()`
      INSERT INTO resource_incarnations (deployment_id, local_owner_id, source_resource_incarnation_id, account_incarnation_id, resource_kind, provider_resource_id, incarnation)
      VALUES (${deploymentId}, 'owner-a', '00000000-0000-0000-0000-000000000030', '00000000-0000-0000-0000-000000000020', 'cash_ledger', 'tx-1', 1)
    `
  })

  afterAll(async () => {
    await sql()?.end()
  })

  it('authority epoch exclusion constraint prevents overlapping primary epochs for same account+domain', async () => {
    await sql()`
      INSERT INTO authority_epochs (
        deployment_id, canonical_account_id, resource_domain, source_account_incarnation_id,
        authority_time_selector, authority_time_selector_version, role, state,
        valid_range, approved_by
      ) VALUES (
        ${deploymentId}, 'acct-canon-1', 'cash_ledger', '00000000-0000-0000-0000-000000000020',
        'posted_date', 1, 'primary', 'active',
        '[2026-01-01, 2026-12-31)', 'owner-a'
      )
    `
    await expect(
      sql()`
        INSERT INTO authority_epochs (
          deployment_id, canonical_account_id, resource_domain, source_account_incarnation_id,
          authority_time_selector, authority_time_selector_version, role, state,
          valid_range, approved_by
        ) VALUES (
          ${deploymentId}, 'acct-canon-1', 'cash_ledger', '00000000-0000-0000-0000-000000000020',
          'posted_date', 1, 'primary', 'active',
          '[2026-06-01, 2027-01-01)', 'owner-a'
        )
      `
    ).rejects.toThrow()
  })

  it('non-overlapping authority epochs for same account+domain are accepted', async () => {
    await expect(
      sql()`
        INSERT INTO authority_epochs (
          deployment_id, canonical_account_id, resource_domain, source_account_incarnation_id,
          authority_time_selector, authority_time_selector_version, role, state,
          valid_range, approved_by
        ) VALUES (
          ${deploymentId}, 'acct-canon-1', 'cash_ledger', '00000000-0000-0000-0000-000000000020',
          'posted_date', 1, 'primary', 'active',
          '[2028-01-01, 2029-01-01)', 'owner-a'
        )
      `
    ).resolves.toBeDefined()
  })

  it('different deployment_id allows overlapping epochs', async () => {
    await expect(
      sql()`
        INSERT INTO authority_epochs (
          deployment_id, canonical_account_id, resource_domain, source_account_incarnation_id,
          authority_time_selector, authority_time_selector_version, role, state,
          valid_range, approved_by
        ) VALUES (
          'other-deploy', 'acct-canon-1', 'cash_ledger', '00000000-0000-0000-0000-000000000020',
          'posted_date', 1, 'primary', 'active',
          '[2026-06-01, 2027-01-01)', 'owner-b'
        )
      `
    ).resolves.toBeDefined()
  })

  it('raw observations are append-only', async () => {
    await sql()`
      INSERT INTO raw_observations (deployment_id, local_owner_id, observation_id, source_resource_incarnation_id, semantic_hash, hash_specification_version, payload, observed_at)
      VALUES (${deploymentId}, 'owner-a', gen_random_uuid(), '00000000-0000-0000-0000-000000000030', ${'a'.repeat(64)}, 1, '{}'::jsonb, now())
    `
    const countResult = await sql()<{ count: number }[]>`SELECT count(*)::int count FROM raw_observations WHERE deployment_id = ${deploymentId}`
    expect(countResult[0].count).toBeGreaterThanOrEqual(1)

    await expect(
      sql()`UPDATE raw_observations SET payload = '{"x":1}'::jsonb WHERE deployment_id = ${deploymentId}`
    ).rejects.toThrow()
  })

  it('deployment_id is NOT NULL on all G2 tables', async () => {
    const tables = ['authority_epochs', 'provider_principals', 'connection_incarnations',
      'account_incarnations', 'resource_incarnations', 'raw_observations',
      'finance_sync_migrations']
    for (const table of tables) {
      const result = await sql()`
        SELECT column_name, is_nullable
        FROM information_schema.columns
        WHERE table_name = ${table} AND column_name = 'deployment_id'
      `
      expect(result.length).toBeGreaterThan(0)
      expect(result[0].is_nullable).toBe('NO')
    }
  })

  it('deployment isolation: cross-deployment query returns empty', async () => {
    const rows = await sql()<{ count: number }[]>`SELECT count(*)::int count FROM authority_epochs WHERE deployment_id = 'nonexistent-deploy'`
    expect(rows[0].count).toBe(0)
  })
})
