import { afterAll, describe, expect, it } from "vitest";
import postgres from "postgres";
import { migrate } from "./migrate";
import { resolve } from "node:path";
import { createRuntimeDependencies } from "../runtime-deps";

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL;
const suite = url ? describe : describe.skip;
const RUN_ID = `${process.pid}-${Date.now()}`;
const DEPLOYMENT_ID = `g2-integrity-test-${RUN_ID}`;

let sql: ReturnType<typeof postgres> | undefined;

suite("G2: raw_observations append-only enforcement", () => {
  it("rejects DELETE on raw_observations via database trigger", async () => {
    sql = postgres(url!, { connect_timeout: 3, prepare: false });

    process.env.FINANCE_SYNC_DEPLOYMENT_ID = DEPLOYMENT_ID;

    // Apply all finance-sync migrations (includes the append-only trigger)
    const directory = resolve(import.meta.dirname, "../../../finance-sync-migrations");
    await migrate(sql, directory);

    // Seed the identity chain that raw_observations depends on
    await sql`
      INSERT INTO provider_principals
        (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', 'test-provider',
         '00000000-0000-0000-0000-000000000001', 'test-subject')
    `;

    await sql`
      INSERT INTO connection_incarnations
        (deployment_id, local_owner_id, connection_incarnation_id,
         provider_principal_id, provider_connection_id, incarnation)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', '00000000-0000-0000-0000-000000000010',
         '00000000-0000-0000-0000-000000000001', 'test-conn', 1)
    `;

    await sql`
      INSERT INTO account_incarnations
        (deployment_id, local_owner_id, account_incarnation_id,
         connection_incarnation_id, provider_account_id, incarnation)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', '00000000-0000-0000-0000-000000000020',
         '00000000-0000-0000-0000-000000000010', 'test-account', 1)
    `;

    await sql`
      INSERT INTO resource_incarnations
        (deployment_id, local_owner_id, source_resource_incarnation_id,
         account_incarnation_id, resource_kind, provider_resource_id, incarnation)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', '00000000-0000-0000-0000-000000000030',
         '00000000-0000-0000-0000-000000000020', 'test_resource', 'test-resource', 1)
    `;

    // Insert a raw_observation to attempt DELETE against
    const OBSERVATION_ID = '00000000-0000-0000-0000-000000000040';
    await sql`
      INSERT INTO raw_observations
        (deployment_id, local_owner_id, observation_id,
         source_resource_incarnation_id, semantic_hash,
         hash_specification_version, payload, observed_at)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', ${OBSERVATION_ID},
         '00000000-0000-0000-0000-000000000030',
         ${'a'.repeat(64)}, 1, ${JSON.stringify({ test: true })}, NOW())
    `;

    // ASSERT — DELETE must be rejected by the append-only trigger
    await expect(
      sql`DELETE FROM raw_observations WHERE deployment_id = ${DEPLOYMENT_ID} AND observation_id = ${OBSERVATION_ID}`
    ).rejects.toThrow(/raw observations are append-only/i);

    // Verify the row was NOT deleted
    const [{ cnt }] = await sql`
      SELECT COUNT(*)::int AS cnt
      FROM raw_observations
      WHERE deployment_id = ${DEPLOYMENT_ID} AND observation_id = ${OBSERVATION_ID}
    `;
    expect(cnt).toBe(1);
  });

  it("proves regression-detection by toggling the trigger on/off", async () => {
    // Reuse the connection from the first test
    if (!sql) throw new Error("no db connection — previous test must run first");

    const OBSERVATION_ID = '00000000-0000-0000-0000-000000000050';

    // Insert one more row for this sub-test
    await sql`
      INSERT INTO raw_observations
        (deployment_id, local_owner_id, observation_id,
         source_resource_incarnation_id, semantic_hash,
         hash_specification_version, payload, observed_at)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', ${OBSERVATION_ID},
         '00000000-0000-0000-0000-000000000030',
         ${'b'.repeat(64)}, 1, ${JSON.stringify({ toggle: true })}, NOW())
    `;

    // --- Prove the trigger is what blocks DELETE ---
    // Disable the trigger
    await sql.unsafe(`
      ALTER TABLE raw_observations DISABLE TRIGGER raw_observations_append_only
    `);

    // DELETE should now succeed
    const delResult = await sql`
      DELETE FROM raw_observations WHERE deployment_id = ${DEPLOYMENT_ID} AND observation_id = ${OBSERVATION_ID}
    `;
    // postgres returns an array with one element containing `count`
    expect(delResult.length).toBe(0); // no RETURNING, so empty result set

    // Row should be gone
    const [{ cnt: afterDelete }] = await sql`
      SELECT COUNT(*)::int AS cnt
      FROM raw_observations
      WHERE deployment_id = ${DEPLOYMENT_ID} AND observation_id = ${OBSERVATION_ID}
    `;
    expect(afterDelete).toBe(0);

    // --- Re-enable trigger for all subsequent tests ---
    await sql.unsafe(`
      ALTER TABLE raw_observations ENABLE TRIGGER raw_observations_append_only
    `);

    // Prove it blocks again with a fresh row
    await sql`
      INSERT INTO raw_observations
        (deployment_id, local_owner_id, observation_id,
         source_resource_incarnation_id, semantic_hash,
         hash_specification_version, payload, observed_at)
      VALUES
        (${DEPLOYMENT_ID}, 'test-owner', ${OBSERVATION_ID},
         '00000000-0000-0000-0000-000000000030',
         ${'c'.repeat(64)}, 1, ${JSON.stringify({ restored: true })}, NOW())
    `;

    await expect(
      sql`DELETE FROM raw_observations WHERE deployment_id = ${DEPLOYMENT_ID} AND observation_id = ${OBSERVATION_ID}`
    ).rejects.toThrow(/raw observations are append-only/i);
  });
});

// ── G8 projection dead-letter integration ─────────────────────────────
suite("G8: projection_outbox dead-letter after retry exhaustion", () => {
  it("transitions a pending outbox item to dead_letter state after MAX_PROJECTION_DELIVERY_ATTEMPTS failures", async () => {
    sql = postgres(url!, { connect_timeout: 3, prepare: false });

    const deadLetterDeploymentId = `g8-dead-letter-test-${RUN_ID}`;
    process.env.FINANCE_SYNC_DEPLOYMENT_ID = deadLetterDeploymentId;

    const directory = resolve(import.meta.dirname, "../../../finance-sync-migrations");
    await migrate(sql, directory);

    // Seed identity chain in an isolated deployment namespace.
    await sql`
      INSERT INTO provider_principals
        (deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id)
      VALUES
        (${deadLetterDeploymentId}, 'test-owner', 'test-provider',
         'a0000000-0000-0000-0000-000000000001', 'test-subject')
    `;
    await sql`
      INSERT INTO connection_incarnations
        (deployment_id, local_owner_id, connection_incarnation_id,
         provider_principal_id, provider_connection_id, incarnation)
      VALUES
        (${deadLetterDeploymentId}, 'test-owner', 'a0000000-0000-0000-0000-000000000010',
         'a0000000-0000-0000-0000-000000000001', 'test-conn', 1)
    `;
    await sql`
      INSERT INTO account_incarnations
        (deployment_id, local_owner_id, account_incarnation_id,
         connection_incarnation_id, provider_account_id, incarnation)
      VALUES
        (${deadLetterDeploymentId}, 'test-owner', 'a0000000-0000-0000-0000-000000000020',
         'a0000000-0000-0000-0000-000000000010', 'test-account', 1)
    `;
    await sql`
      INSERT INTO resource_incarnations
        (deployment_id, local_owner_id, source_resource_incarnation_id,
         account_incarnation_id, resource_kind, provider_resource_id, incarnation)
      VALUES
        (${deadLetterDeploymentId}, 'test-owner', 'a0000000-0000-0000-0000-000000000030',
         'a0000000-0000-0000-0000-000000000020', 'test_resource', 'test-resource', 1)
    `;

    // Seed a projection_outbox row with attempts = 2 (one more failure → dead-letter)
    const outboxPayload = {
      idempotencyKey: "dlq-int-key-1",
      subject: "test-owner",
      kind: "ActivityUpsert",
      resourceId: "dep-1:res-1:txn-1",
      accessGeneration: 1,
    };
    await sql`
      INSERT INTO projection_outbox
        (deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload, attempts)
      VALUES
        (${deadLetterDeploymentId}, 'test-owner', 'test-conn', 1, 'dlq-int-key-1',
         ${JSON.stringify(outboxPayload)}, 2)
    `;

    const connection = { provider: 'plaid' as const, connectionId: 'test-conn', deploymentId: deadLetterDeploymentId, clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test', localOwnerId: 'urn:test#owner', sourceResourceIncarnationId: 'a0000000-0000-0000-0000-000000000030', accountIds: ['test-account'], wealthfolioAccountIds: { 'test-account': 'test-account' }, providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT' };
    const { accessToken: _accessToken, clientId: _clientId, clientSecret: _clientSecret, ...connectionMetadata } = connection;
    // Fully configured, allowlisted HTTPS endpoint guaranteed to refuse the connection.
    const deps = createRuntimeDependencies(sql, deadLetterDeploymentId, {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([connectionMetadata]),
      FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify({ 'test-conn': {
        accessToken: connection.accessToken, clientId: connection.clientId, clientSecret: connection.clientSecret,
      } }),
      FINANCE_SYNC_PLAID_URL: "https://sandbox.plaid.com",
      FINANCE_SYNC_BRIDGE_URL: "https://127.0.0.1:1",
      FINANCE_SYNC_BRIDGE_RECEIPT_URL: "https://127.0.0.1:1/receipts",
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: "127.0.0.1",
      FINANCE_SYNC_BRIDGE_TOKEN: "test-token",
    });

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response('{"error":"rejected"}', { status: 400 })) as typeof fetch;
    let result;
    try {
      result = await deps.projectPending(connection);
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(result.delivered).toBe(0);
    expect(result.failed).toBe(1);

    // Verify the row is now dead-lettered
    const [row] = await sql`
      SELECT state, attempts, dead_lettered_at, dead_letter_reason
      FROM projection_outbox
      WHERE idempotency_key = 'dlq-int-key-1'
    `;
    expect(row.state).toBe("dead_letter");
    expect(row.attempts).toBe(3);
    expect(row.dead_lettered_at).not.toBeNull();
    expect(row.dead_letter_reason).toBeTruthy();
  });
});

afterAll(async () => {
  await sql?.end();
});
