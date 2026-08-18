import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { discoverMigrations, verifyAppliedMigrations } from "./migrate";

const root = resolve(import.meta.dirname, "../../../finance-sync-migrations");

describe("finance-sync migrations", () => {
  it("discovers migrations in numeric order with sha256 checksums", async () => {
    const migrations = await discoverMigrations(root);
    expect(migrations.map((m) => m.version)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    expect(migrations.every((m) => /^[a-f0-9]{64}$/.test(m.checksum))).toBe(true);
  });

  it("defines Plaid item-level checkpoints", () => {
    const sql = readFileSync(resolve(root, "0016_plaid_item_checkpoints.sql"), "utf8");
    expect(sql).toMatch(/CREATE TABLE IF NOT EXISTS plaid_item_checkpoints/i);
    expect(sql).toMatch(/PRIMARY KEY \(deployment_id, local_owner_id, provider_connection_id, provider_generation\)/i);
    expect(sql).toMatch(/provider_generation integer NOT NULL/i);
  });

  it("defines generation-bound admissions, checkpoint/outbox identities, and rotation supersession", () => {
    const sql = readFileSync(resolve(root, "0009_provider_generation_fences.sql"), "utf8");
    expect(sql).toMatch(/provider_account_admissions[\s\S]*provider_generation/i);
    expect(sql).toMatch(/PRIMARY KEY \(deployment_id, connection_id, provider_generation\)/i);
    expect(sql).toMatch(/UNIQUE \(deployment_id, connection_id, provider_generation, idempotency_key\)/i);
    expect(sql).not.toMatch(/ADD COLUMN IF NOT EXISTS provider_generation integer NOT NULL DEFAULT 1/i);
    expect(sql).toMatch(/UPDATE sync_checkpoints[\s\S]*LIKE pc\.connection_id::text \|\| ':%'/i);
    expect(sql).toMatch(/legacy projection outbox row has no unambiguous provider connection generation/i);
    expect(sql).toMatch(/state = 'dead_letter'[\s\S]*PROVIDER_GENERATION_SUPERSEDED/i);
    expect(sql).toMatch(/provider_generation = OLD\.provider_generation/i);
  });

  it("binds each Plaid continuation to one reserved slot", () => {
    const sql = readFileSync(resolve(root, "0010_plaid_connect_slot_binding.sql"), "utf8");
    expect(sql).toMatch(/ADD COLUMN IF NOT EXISTS selected_secret_reference text/i);
    expect(sql).toMatch(/ALTER COLUMN selected_secret_reference SET NOT NULL/i);
    expect(sql).toMatch(/provider_connect_sessions_live_slot_idx/i);
    expect(sql).toMatch(/selected_secret_reference\)\s*WHERE released_at IS NULL/i);
  });

  it("rebinds active admissions and unresolved bridge work to the latest receiver access generation", () => {
    const sql = readFileSync(resolve(root, "0011_receiver_access_generation_rebind.sql"), "utf8");
    expect(sql).toMatch(/max\(paa\.receiver_access_generation\)/i);
    expect(sql).toMatch(/UPDATE provider_account_admissions paa[\s\S]*receiver_access_generation = latest\.receiver_access_generation/i);
    expect(sql).toMatch(/jsonb_set\([\s\S]*'\{accessGeneration\}'/i);
    expect(sql).toMatch(/dead_letter_reason = 'BRIDGE_HTTP_REJECTED'/i);
    expect(sql).toMatch(/active\.connection_id::text = po\.connection_id/i);
    expect(sql).toMatch(/active\.wealthfolio_account_id = po\.payload->>'accountId'/i);
    expect(sql).toMatch(/pc\.provider_generation = active\.provider_generation/i);
  });

  it("repairs stale runtime connection identities that include the provider account suffix", () => {
    const sql = readFileSync(resolve(root, "0012_runtime_connection_receiver_rebind.sql"), "utf8");
    expect(sql).toMatch(/po\.connection_id LIKE active\.connection_id::text \|\| ':%'/i);
    expect(sql).toMatch(/active\.wealthfolio_account_id = po\.payload->>'accountId'/i);
    expect(sql).toMatch(/dead_letter_reason = 'BRIDGE_HTTP_REJECTED'/i);
    expect(sql).toMatch(/state = 'pending'/i);
  });

  it("defines an append-only exact observation quarantine and suppressible outbox state", () => {
    const sql = readFileSync(resolve(root, "0014_observation_quarantine.sql"), "utf8");
    expect(sql).toMatch(/CREATE TABLE IF NOT EXISTS observation_quarantines/i);
    expect(sql).toMatch(/UNIQUE \([\s\S]*provider_version, semantic_hash, hash_specification_version/i);
    expect(sql).toMatch(/FOREIGN KEY \(deployment_id, local_owner_id, observation_id\)[\s\S]*REFERENCES raw_observations/i);
    expect(sql).toMatch(/REVOKE UPDATE, DELETE ON observation_quarantines FROM finance_sync_runtime/i);
    expect(sql).toMatch(/observation quarantines are append-only/i);
    expect(sql).toMatch(/'suppressed'/i);
  });

  it("defines append-only Card Code ingress quarantine", () => {
    const sql = readFileSync(resolve(root, "0015_card_ingress_quarantine.sql"), "utf8");
    expect(sql).toMatch(/CREATE TABLE IF NOT EXISTS card_ingress_quarantines/i);
    expect(sql).toMatch(/envelope jsonb NOT NULL/i);
    expect(sql).toMatch(/REVOKE UPDATE, DELETE ON card_ingress_quarantines FROM finance_sync_runtime/i);
    expect(sql).toMatch(/card ingress quarantines are append-only/i);
  });

  it("rejects checksum drift", () => {
    expect(() => verifyAppliedMigrations([{ version: 1, checksum: "old" }], [{ version: 1, name: "identity", checksum: "new", sql: "" }])).toThrow(/checksum drift/i);
  });

  it("scopes identity and observation constraints", () => {
    const sql = ["0001_identity.sql", "0002_observations.sql"].map((f) => readFileSync(resolve(root, f), "utf8")).join("\n");
    for (const table of ["provider_principals", "connection_incarnations", "account_incarnations", "resource_incarnations", "raw_observations"])
      expect(sql).toMatch(new RegExp(`CREATE TABLE( IF NOT EXISTS)? ${table}[^;]*deployment_id`, "is"));
    expect(sql).toMatch(/UNIQUE \(deployment_id, local_owner_id, provider, provider_principal_id\)/i);
    expect(sql).toMatch(/FOREIGN KEY \(deployment_id, local_owner_id, source_resource_incarnation_id\)/i);
    expect(sql).toMatch(/REVOKE UPDATE, DELETE ON raw_observations FROM finance_sync_runtime/i);
  });
});
