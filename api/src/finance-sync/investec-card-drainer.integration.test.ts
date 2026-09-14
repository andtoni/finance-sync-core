import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { reprojectRetainedInvestecCards } from './investec-card-drainer'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
let sql: ReturnType<typeof postgres>

suite('retained Investec card reprojection with PostgreSQL', () => {
  beforeAll(async () => {
    sql = postgres(url!, { prepare: false })
    await sql.unsafe('CREATE SCHEMA retained_card_distinct_test')
  })

  afterAll(async () => {
    await sql.unsafe('DROP SCHEMA IF EXISTS retained_card_distinct_test CASCADE')
    await sql.end()
  })

  it('executes the latest-row query with a synthetic retained observation', async () => {
    const schema = 'retained_card_distinct_test'
    await sql.unsafe(`
      CREATE TABLE "${schema}".raw_observations (
        observation_id bigserial PRIMARY KEY, deployment_id text NOT NULL,
        local_owner_id text NOT NULL, source_resource_incarnation_id uuid NOT NULL,
        provider_version text NOT NULL, payload jsonb NOT NULL, raw_payload jsonb,
        semantic_hash text NOT NULL, hash_specification_version integer NOT NULL,
        observed_at timestamptz NOT NULL, recorded_at timestamptz NOT NULL
      );
      CREATE TABLE "${schema}".observation_quarantines (
        deployment_id text NOT NULL, local_owner_id text NOT NULL,
        source_resource_incarnation_id uuid NOT NULL, provider_version text NOT NULL,
        semantic_hash text NOT NULL, hash_specification_version integer NOT NULL
      );
      INSERT INTO "${schema}".raw_observations (
        deployment_id, local_owner_id, source_resource_incarnation_id,
        provider_version, payload, raw_payload, semantic_hash,
        hash_specification_version, observed_at, recorded_at
      ) VALUES (
        'synthetic-deployment', 'synthetic-owner', '00000000-0000-4000-8000-000000000001',
        'investec:card:synthetic', '{"transactionId":"synthetic-transaction","accountId":"synthetic-account"}',
        '{"accountId":"synthetic-account"}', repeat('a', 64), 5,
        '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
      )
    `)

    const isolatedSql = await sql.reserve()
    await isolatedSql`SET search_path TO ${sql.unsafe(`"${schema}"`)}, public`
    const deps: any = { loadConnections: async () => [] }
    await expect(reprojectRetainedInvestecCards(isolatedSql as any, 'synthetic-deployment', deps))
      .rejects.toThrow('Investec retained card projection is unresolved')
    await isolatedSql.release()
  })
})
