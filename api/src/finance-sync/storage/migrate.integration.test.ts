import { afterAll, beforeAll, describe, expect, it } from "vitest";
import postgres from "postgres";
import { discoverMigrations, migrate } from "./migrate";
import { resolve } from "node:path";
import { assignProjectedRevision } from "../projection/projected-revision";
import type { BridgeDisclosureOperation } from "../projection/bridge-client";
import type { FinanceSyncSql } from "./pg";

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

  it("preserves populated raw observations while adding observation revisions", async () => {
    const schemaName = `finance_sync_revision_upgrade_${process.pid}_${Date.now()}`;
    await withSchema(schemaName, async tx => {
      const migrations = await applyThroughEight(tx, schemaName);
      for (const migration of migrations.filter(candidate => candidate.version > 8 && candidate.version <= 20)) {
        await tx.unsafe(migration.sql);
      }
      const deploymentId = 'revision-upgrade-deployment';
      const owner = 'urn:wealthfolio:test#revision-owner';
      const principalId = '51000000-0000-4000-8000-000000000001';
      const connectionId = '52000000-0000-4000-8000-000000000002';
      const accountIncarnationId = '53000000-0000-4000-8000-000000000003';
      const sourceId = '54000000-0000-4000-8000-000000000004';
      await tx`INSERT INTO provider_principals (deployment_id,local_owner_id,provider,provider_principal_id,provider_subject_id)
        VALUES (${deploymentId},${owner},'investec',${principalId}::uuid,'revision-subject')`;
      await tx`INSERT INTO connection_incarnations (deployment_id,local_owner_id,connection_incarnation_id,provider_principal_id,provider_connection_id,incarnation)
        VALUES (${deploymentId},${owner},${connectionId}::uuid,${principalId}::uuid,'revision-connection',1)`;
      await tx`INSERT INTO account_incarnations (deployment_id,local_owner_id,account_incarnation_id,connection_incarnation_id,provider_account_id,incarnation)
        VALUES (${deploymentId},${owner},${accountIncarnationId}::uuid,${connectionId}::uuid,'revision-account',1)`;
      await tx`INSERT INTO resource_incarnations (deployment_id,local_owner_id,source_resource_incarnation_id,account_incarnation_id,resource_kind,provider_resource_id,incarnation)
        VALUES (${deploymentId},${owner},${sourceId}::uuid,${accountIncarnationId}::uuid,'cash_ledger','revision-resource',1)`;
      const payload = { transactionId: 'revision-fact', accountId: 'revision-account', amount: '-1', currency: 'ZAR' };
      await tx`INSERT INTO raw_observations (deployment_id,local_owner_id,observation_id,source_resource_incarnation_id,provider_version,semantic_hash,hash_specification_version,payload,observed_at)
        VALUES (${deploymentId},${owner},'55000000-0000-4000-8000-000000000005'::uuid,${sourceId}::uuid,'investec:txn:revision-fact',${'a'.repeat(64)},1,${tx.json(payload)},'2026-09-14T00:00:00Z')`;
      const legacyPayload = { ...payload, transactionId: 'legacy-null-provider' };
      await tx`INSERT INTO raw_observations (deployment_id,local_owner_id,observation_id,source_resource_incarnation_id,provider_version,semantic_hash,hash_specification_version,payload,observed_at)
        VALUES (${deploymentId},${owner},'56000000-0000-4000-8000-000000000006'::uuid,${sourceId}::uuid,NULL,${'b'.repeat(64)},1,${tx.json(legacyPayload)},'2026-09-14T00:00:01Z')`;
      await tx.unsafe(migrations.find(migration => migration.version === 21)!.sql);
      const raw = await tx`SELECT payload, observation_id, provider_version FROM raw_observations WHERE deployment_id=${deploymentId} ORDER BY observation_id`;
      const revisions = await tx`SELECT observation_revision FROM observation_revision_ledger WHERE deployment_id=${deploymentId}`;
      expect(raw).toHaveLength(2);
      expect(raw[0].payload).toEqual(payload);
      expect(raw[1].payload).toEqual(legacyPayload);
      expect(raw[1].provider_version).toBeNull();
      expect(revisions).toHaveLength(1);
      expect(Number(revisions[0].observation_revision)).toBeGreaterThan(0);
    });
  });

  it("seeds projected activity revisions from enqueued work so the first projection after upgrade stays ordered", async () => {
    const schemaName = `finance_sync_projected_revision_${process.pid}_${Date.now()}`;
    await withSchema(schemaName, async tx => {
      const migrations = await applyThroughEight(tx, schemaName);
      for (const migration of migrations.filter(candidate => candidate.version > 8 && candidate.version <= 22)) {
        await tx.unsafe(migration.sql);
      }
      const deploymentId = 'projected-revision-upgrade';
      const owner = 'urn:wealthfolio:test#projected-owner';
      const resource = (id: string) => `${deploymentId}:60000000-0000-4000-8000-000000000001:${id}`;
      const activity = (notes: string) => ({
        accountId: 'wealthfolio-account', activityType: 'WITHDRAWAL', status: 'POSTED', activityDate: '2026-09-14',
        activityDatePrecision: 'DATE' as const, amount: '12', currency: 'ZAR', notes,
        metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } },
      });
      const operation = (key: string, resourceId: string, notes: string, extra: Partial<BridgeDisclosureOperation> = {}): BridgeDisclosureOperation => ({
        deploymentId, accountId: 'wealthfolio-account', subject: owner, accessGeneration: 1, kind: 'ActivityUpsert',
        idempotencyKey: key, resourceId, activity: activity(notes), ...extra,
      });
      const enqueue = async (payload: BridgeDisclosureOperation, state: string) => (await tx<{ id: string }[]>`
        INSERT INTO projection_outbox (deployment_id,local_owner_id,connection_id,provider_generation,idempotency_key,payload,state)
        VALUES (${deploymentId},${owner},'legacy-connection',1,${payload.idempotencyKey},${tx.json(payload as never)},${state})
        RETURNING id`)[0].id;
      // One payload at two revisions, then a settlement delivered on its own and
      // a dead-lettered correlation of it at the same revision.
      await enqueue(operation('plain-41', resource('plain'), 'plain', { observationRevision: 41 }), 'delivered');
      await enqueue(operation('plain-44', resource('plain'), 'plain', { observationRevision: 44 }), 'delivered');
      await enqueue(operation('settled-alone', resource('posted'), 'settlement', { observationRevision: 47 }), 'delivered');
      const deadLetter = await enqueue(operation('correlated', resource('card'), 'correlated', {
        observationRevision: 47, lifecycleSuccessorId: resource('posted'),
      }), 'dead_letter');
      await enqueue(operation('unrevisioned', resource('unrevisioned'), 'legacy'), 'delivered');
      await enqueue({ ...operation('removed', resource('removed'), 'removed', { observationRevision: 48 }), kind: 'ActivityDelete', activity: undefined }, 'delivered');
      await tx`INSERT INTO investec_terminal_revision_recovery_intents
        (deployment_id,local_owner_id,outbox_id,expected_payload_hash,reason,actor_subject,mode,state,ordinary_operation)
        VALUES (${deploymentId},${owner},${deadLetter}::uuid,${'a'.repeat(64)},'recovered','operator','apply','suppressed',
          ${tx.json(operation('recovered', resource('recovered'), 'recovered', { observationRevision: 49 }) as never)})`;
      // Fresh ordinals come from the sequence that issued the legacy revisions.
      await tx`SELECT setval('finance_sync_observation_revision_seq', 60)`;

      await tx.unsafe(migrations.find(migration => migration.version === 23)!.sql);

      const digest = async (notes: string) => (await tx<{ digest: string }[]>`
        SELECT encode(sha256(convert_to((${tx.json(activity(notes))}::jsonb)::text, 'UTF8')), 'hex') AS digest`)[0].digest;
      const seeded = await tx<{ resource_id: string; observation_revision: string; payload_digest: string | null }[]>`
        SELECT resource_id, observation_revision, payload_digest FROM projected_activity_revision_ledger
        WHERE deployment_id=${deploymentId} ORDER BY observation_revision, resource_id`;
      expect(seeded).toEqual([
        { resource_id: resource('plain'), observation_revision: '41', payload_digest: await digest('plain') },
        { resource_id: resource('plain'), observation_revision: '44', payload_digest: await digest('plain') },
        { resource_id: resource('posted'), observation_revision: '47', payload_digest: null },
        { resource_id: resource('recovered'), observation_revision: '49', payload_digest: await digest('recovered') },
      ]);
      const [untouched] = await tx<{ rows: number; states: string[] }[]>`
        SELECT count(*)::int AS rows, array_agg(DISTINCT state ORDER BY state) AS states
        FROM projection_outbox WHERE deployment_id=${deploymentId}`;
      expect(untouched).toEqual({ rows: 6, states: ['dead_letter', 'delivered'] });

      const assign = (candidate: BridgeDisclosureOperation) => assignProjectedRevision(tx as unknown as FinanceSyncSql, candidate);
      // The payload last delivered keeps its revision and key, even from an older ordinal.
      const replayed = operation('plain-44', resource('plain'), 'plain', { observationRevision: 44 });
      expect(await assign(replayed)).toBe(replayed);
      expect((await assign(operation('plain-41', resource('plain'), 'plain', { observationRevision: 41 }))).observationRevision).toBe(44);
      // A different payload at the delivered revision takes a fresh ordinal.
      const changed = await assign(operation('plain-44', resource('plain'), 'changed', { observationRevision: 44 }));
      expect(changed.observationRevision).toBe(61);
      expect(changed.idempotencyKey).not.toBe('plain-44');
      // Neither payload of the conflicting legacy revision may reuse it.
      const settled = await assign(operation('settled-alone', resource('posted'), 'settlement', { observationRevision: 47 }));
      expect(settled.observationRevision).toBe(62);
      const correlated = await assign(operation('correlated', resource('card'), 'correlated', {
        observationRevision: 47, lifecycleSuccessorId: resource('posted'),
      }));
      expect(correlated.observationRevision).toBe(63);
      expect(correlated.idempotencyKey).not.toBe('correlated');
      // A genuinely newer observation ordinal is kept as is.
      const newer = operation('recovered-next', resource('recovered'), 'revised', { observationRevision: 55 });
      expect(await assign(newer)).toBe(newer);
      // The lifecycle predecessor is not a written activity, so it has no history.
      const predecessor = operation('card-pending', resource('card'), 'authorization', { observationRevision: 40 });
      expect(await assign(predecessor)).toBe(predecessor);
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
    expect(rows.map((r) => r.version)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]);
    const revisionLedger = await sql!`
      select table_name from information_schema.tables
      where table_name = 'observation_revision_ledger'
    `;
    expect(revisionLedger).toHaveLength(1);
    const rawColumns = await sql!`
      select column_name from information_schema.columns
      where table_name = 'raw_observations' and column_name = 'raw_payload'
    `;
    expect(rawColumns).toHaveLength(1);
    const cardIngressQuarantine = await sql!`
      select table_name from information_schema.tables
      where table_name = 'card_ingress_quarantines'
    `;
    expect(cardIngressQuarantine).toHaveLength(1);
    const plaidSessionColumns = await sql!`
      select column_name from information_schema.columns
      where table_name = 'provider_connect_sessions' and column_name in ('flow_kind', 'provider_connection_id', 'provider_generation')
    `;
    expect(plaidSessionColumns.map((row) => row.column_name).sort()).toEqual(['flow_kind', 'provider_connection_id', 'provider_generation']);

  });

  it("rejects an intentional legacy checksum conflict in an isolated schema", async () => {
    const schemaName = `finance_sync_checksum_${process.pid}_${Date.now()}`;
    const schema = quoteIdentifier(schemaName);
    await sql!.unsafe(`CREATE SCHEMA ${schema}`);
    const isolated = postgres(url!, {
      connect_timeout: 3,
      prepare: false,
      max: 1,
      connection: { search_path: `${schemaName},public` },
    });
    try {
      await migrate(isolated, directory);
      await isolated`insert into finance_sync_migrations (deployment_id, version, name, checksum)
        values ('conflicting-legacy', 1, 'identity', ${'0'.repeat(64)})`;
      await expect(migrate(isolated, directory)).rejects.toThrow(/checksum conflict/i);
    } finally {
      await isolated.end();
      await sql!.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    }
  });

  it("enforces lifecycle override one-to-one and append-only constraints in PostgreSQL", async () => {
    const schemaName = `finance_sync_lifecycle_${process.pid}_${Date.now()}`;
    const schema = quoteIdentifier(schemaName);
    await sql!.unsafe(`CREATE SCHEMA ${schema}`);
    const first = postgres(url!, {
      connect_timeout: 3,
      prepare: false,
      max: 1,
      connection: { search_path: schemaName },
    });
    const second = postgres(url!, {
      connect_timeout: 3,
      prepare: false,
      max: 1,
      connection: { search_path: schemaName },
    });
    try {
      await first.begin(async transaction => {
        const tx = transaction as unknown as ReturnType<typeof postgres>;
        const migrations = await applyThroughEight(tx, schemaName);
        for (const migration of migrations.filter(candidate => candidate.version > 8)) {
          await tx.unsafe(migration.sql);
        }
      });
      const deploymentId = "lifecycle-test";
      const owner = "urn:wealthfolio:test#lifecycle-owner";
      const principalId = "10000000-0000-4000-8000-000000000101";
      const connectionId = "20000000-0000-4000-8000-000000000102";
      const accountIncarnationId = "30000000-0000-4000-8000-000000000103";
      const sourceId = "40000000-0000-4000-8000-000000000104";
      await first.begin(async transaction => {
        const tx = transaction as unknown as ReturnType<typeof postgres>;
        await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
        await tx`INSERT INTO provider_principals
          (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
          VALUES (${deploymentId}, ${owner}, 'investec', ${principalId}::uuid, 'lifecycle-subject')`;
        await tx`INSERT INTO connection_incarnations
          (deployment_id, local_owner_id, connection_incarnation_id, provider_principal_id,
           provider_connection_id, incarnation)
          VALUES (${deploymentId}, ${owner}, ${connectionId}::uuid, ${principalId}::uuid,
            'lifecycle-connection', 1)`;
        await tx`INSERT INTO account_incarnations
          (deployment_id, local_owner_id, account_incarnation_id, connection_incarnation_id,
           provider_account_id, incarnation)
          VALUES (${deploymentId}, ${owner}, ${accountIncarnationId}::uuid, ${connectionId}::uuid,
            'lifecycle-account', 1)`;
        await tx`INSERT INTO resource_incarnations
          (deployment_id, local_owner_id, source_resource_incarnation_id, account_incarnation_id,
           resource_kind, provider_resource_id, incarnation)
          VALUES (${deploymentId}, ${owner}, ${sourceId}::uuid, ${accountIncarnationId}::uuid,
            'card', 'lifecycle-card', 1)`;
      });

      const insertPair = (
        client: ReturnType<typeof postgres>,
        pendingTransactionId: string,
        postedTransactionId: string,
      ) => client.begin(async transaction => {
        const tx = transaction as unknown as ReturnType<typeof postgres>;
        await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
        return tx<{ override_id: string }[]>`INSERT INTO investec_card_lifecycle_overrides (
            deployment_id, local_owner_id, source_resource_incarnation_id,
            pending_transaction_id, posted_transaction_id, actor_subject, reason
          ) VALUES (
            ${deploymentId}, ${owner}, ${sourceId}::uuid,
            ${pendingTransactionId}, ${postedTransactionId}, ${owner}, 'integration test'
          ) RETURNING override_id`;
      });
      const [left, right] = await Promise.all([
        insertPair(first, "shared-fact", "posted-a"),
        insertPair(second, "pending-b", "shared-fact"),
      ]);
      expect(left.length + right.length).toBe(1);

      const [stored] = await first.begin(async transaction => {
        const tx = transaction as unknown as ReturnType<typeof postgres>;
        await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
        return tx<{ override_id: string }[]>`
          SELECT override_id FROM investec_card_lifecycle_overrides`;
      });
      await expect(first.begin(async transaction => {
        const tx = transaction as unknown as ReturnType<typeof postgres>;
        await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
        await tx`UPDATE investec_card_lifecycle_overrides
          SET reason = 'mutated' WHERE override_id = ${stored.override_id}::uuid`;
      })).rejects.toThrow(/append-only/i);
      await expect(first.begin(async transaction => {
        const tx = transaction as unknown as ReturnType<typeof postgres>;
        await tx.unsafe(`SET LOCAL search_path TO ${schema}, pg_catalog`);
        await tx`DELETE FROM investec_card_lifecycle_overrides
          WHERE override_id = ${stored.override_id}::uuid`;
      })).rejects.toThrow(/append-only/i);
    } finally {
      await Promise.all([first.end(), second.end()]);
      await sql!.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    }
  });
});
afterAll(async () => { await sql?.end(); });
