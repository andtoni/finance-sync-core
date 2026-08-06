import { afterAll, beforeAll, describe, expect, it } from "vitest";
import postgres from "postgres";
import { discoverMigrations, migrate } from "./migrate";
import { resolve } from "node:path";

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL;
const suite = url ? describe : describe.skip;
const directory = resolve(import.meta.dirname, "../../../finance-sync-migrations");
let sql: ReturnType<typeof postgres> | undefined;

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

async function withSchema(name: string, run: (tx: ReturnType<typeof postgres>) => Promise<void>): Promise<void> {
  const schema = quoteIdentifier(name);
  await sql!.unsafe(`CREATE SCHEMA ${schema}`);
  try {
    await sql!.begin(async transaction => {
      const tx = transaction as unknown as ReturnType<typeof postgres>;
      await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
      await run(tx);
    });
  } finally {
    await sql!.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
  }
}

async function applyThroughEight(tx: ReturnType<typeof postgres>, schemaName: string): Promise<Awaited<ReturnType<typeof discoverMigrations>>> {
  const migrations = await discoverMigrations(directory);
  for (const migration of migrations.filter(candidate => candidate.version <= 8)) {
    if (migration.version !== 3) {
      await tx.unsafe(migration.sql);
      continue;
    }
    // btree_gist may already live outside this isolated test schema. Qualify the
    // table while exposing the extension's operator class only for migration 3.
    const [extension] = await tx<{ schema_name: string }[]>`
      SELECT ns.nspname AS schema_name
      FROM pg_extension extension
      JOIN pg_namespace ns ON ns.oid = extension.extnamespace
      WHERE extension.extname = 'btree_gist'
    `;
    const extensionSchema = quoteIdentifier(extension.schema_name);
    const schema = quoteIdentifier(schemaName);
    await tx.unsafe(`SET LOCAL search_path TO ${schema}, ${extensionSchema}, pg_catalog`);
    await tx.unsafe(migration.sql
      .replace('CREATE TABLE IF NOT EXISTS authority_epochs', `CREATE TABLE IF NOT EXISTS ${schema}.authority_epochs`)
      .replace('ON authority_epochs(deployment_id', `ON ${schema}.authority_epochs(deployment_id`));
    await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
  }
  return migrations;
}

suite("PostgreSQL migrations (integration)", () => {
  beforeAll(async () => {
    sql = postgres(url!, { connect_timeout: 3, prepare: false });
    await sql`select 1`;
    await sql.begin(async transaction => {
      await transaction`SELECT pg_advisory_xact_lock(hashtextextended('finance-sync-migrations', 0))`;
      await transaction`CREATE EXTENSION IF NOT EXISTS btree_gist`;
    });
  });

  it("upgrades legacy runtime rows to their current provider generation and fences them on rotation", async () => {
    const schemaName = `finance_sync_upgrade_${process.pid}_${Date.now()}`;
    await withSchema(schemaName, async tx => {
      const migrations = await applyThroughEight(tx, schemaName);
      const deploymentId = 'upgrade-deployment';
      const owner = 'urn:wealthfolio:test#upgrade-owner';
      const principalId = '10000000-0000-4000-8000-000000000001';
      const connectionId = '20000000-0000-4000-8000-000000000002';
      const runtimeConnectionId = `${connectionId}:provider-account`;

      await tx`INSERT INTO provider_principals
        (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
        VALUES (${deploymentId}, ${owner}, 'plaid', ${principalId}::uuid, 'upgrade-subject')`;
      await tx`INSERT INTO provider_connections
        (deployment_id, local_owner_id, connection_id, provider_principal_id, provider,
         provider_connection_id, secret_reference, provider_generation, state)
        VALUES (${deploymentId}, ${owner}, ${connectionId}::uuid, ${principalId}::uuid, 'plaid',
          'upgrade-provider-connection', 'secret-v4', 4, 'active')`;
      await tx`INSERT INTO sync_checkpoints (deployment_id, connection_id, provider, cursor)
        VALUES (${deploymentId}, ${runtimeConnectionId}, 'plaid', 'legacy-cursor')`;
      await tx`INSERT INTO projection_outbox
        (deployment_id, local_owner_id, connection_id, idempotency_key, payload, state)
        VALUES (${deploymentId}, ${owner}, ${runtimeConnectionId}, 'legacy-work', '{}'::jsonb, 'uncertain')`;

      await tx.unsafe(migrations.find(migration => migration.version === 9)!.sql);

      const [checkpoint] = await tx`
        SELECT provider_generation, cursor FROM sync_checkpoints
        WHERE deployment_id = ${deploymentId} AND connection_id = ${runtimeConnectionId}`;
      const [outbox] = await tx`
        SELECT provider_generation, state FROM projection_outbox
        WHERE deployment_id = ${deploymentId} AND idempotency_key = 'legacy-work'`;
      expect(checkpoint).toEqual({ provider_generation: 4, cursor: 'legacy-cursor' });
      expect(outbox).toEqual({ provider_generation: 4, state: 'uncertain' });

      await tx`INSERT INTO sync_checkpoints
        (deployment_id, connection_id, provider_generation, provider, cursor)
        VALUES (${deploymentId}, ${runtimeConnectionId}, 4, 'plaid', 'resumed-cursor')
        ON CONFLICT (deployment_id, connection_id, provider_generation)
        DO UPDATE SET cursor = EXCLUDED.cursor`;
      const processable = await tx`
        SELECT id FROM projection_outbox
        WHERE deployment_id = ${deploymentId} AND connection_id = ${runtimeConnectionId}
          AND provider_generation = 4 AND state IN ('pending', 'uncertain')`;
      expect(processable).toHaveLength(1);

      await tx`UPDATE provider_connections
        SET provider_generation = 5, secret_reference = 'secret-v5'
        WHERE deployment_id = ${deploymentId} AND connection_id = ${connectionId}::uuid`;
      const [fenced] = await tx`
        SELECT state, dead_letter_reason FROM projection_outbox
        WHERE deployment_id = ${deploymentId} AND idempotency_key = 'legacy-work'`;
      expect(fenced).toEqual({
        state: 'dead_letter',
        dead_letter_reason: 'PROVIDER_GENERATION_SUPERSEDED',
      });
    });
  });

  it("fails closed when a legacy runtime row has no matching provider connection", async () => {
    const schemaName = `finance_sync_unmatched_${process.pid}_${Date.now()}`;
    await expect(withSchema(schemaName, async tx => {
      const migrations = await applyThroughEight(tx, schemaName);
      await tx`INSERT INTO sync_checkpoints (deployment_id, connection_id, provider, cursor)
        VALUES ('unmatched-deployment', '30000000-0000-4000-8000-000000000003:account', 'plaid', 'cursor')`;
      await tx.unsafe(migrations.find(migration => migration.version === 9)!.sql);
    })).rejects.toThrow(/legacy sync checkpoint has no unambiguous provider connection generation/i);
  });

  it("connects and applies all migrations idempotently", async () => {
    process.env.FINANCE_SYNC_DEPLOYMENT_ID = "integration-test";
    await migrate(sql!, directory);
    await migrate(sql!, directory);
    const rows = await sql!`select version from finance_sync_schema_migrations order by version`;
    expect(rows.map((r) => r.version)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
    const rawColumns = await sql!`
      select column_name from information_schema.columns
      where table_name = 'raw_observations' and column_name = 'raw_payload'
    `;
    expect(rawColumns).toHaveLength(1);

    await sql!`insert into finance_sync_migrations (deployment_id, version, name, checksum) values ('conflicting-legacy', 1, 'identity', ${'0'.repeat(64)})`;
    await expect(migrate(sql!, directory)).rejects.toThrow(/checksum conflict/i);
    await sql!`delete from finance_sync_migrations where deployment_id = 'conflicting-legacy'`;
  });
});
afterAll(async () => { await sql?.end(); });
