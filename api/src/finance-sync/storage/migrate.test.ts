import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { discoverMigrations, verifyAppliedMigrations } from "./migrate";

const root = resolve(import.meta.dirname, "../../../finance-sync-migrations");

describe("finance-sync migrations", () => {
  it("discovers migrations in numeric order with sha256 checksums", async () => {
    const migrations = await discoverMigrations(root);
    expect(migrations.map((m) => m.version)).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(migrations.every((m) => /^[a-f0-9]{64}$/.test(m.checksum))).toBe(true);
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
