import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import type { FinanceSyncSql } from "./pg";

export interface Migration { version: number; name: string; checksum: string; sql: string }
export interface AppliedMigration { version: number; checksum: string }

export async function discoverMigrations(directory: string): Promise<Migration[]> {
  const files = (await readdir(directory)).filter((f) => /^\d{4}_[a-z0-9_]+\.sql$/.test(f)).sort();
  const migrations = await Promise.all(files.map(async (file) => {
    const sql = await readFile(`${directory}/${file}`, "utf8");
    return { version: Number(file.slice(0, 4)), name: file.slice(5, -4), checksum: createHash("sha256").update(sql).digest("hex"), sql };
  }));
  if (new Set(migrations.map((m) => m.version)).size !== migrations.length) throw new Error("duplicate migration version");
  return migrations;
}

export function verifyAppliedMigrations(applied: AppliedMigration[], available: Migration[]): void {
  for (const row of applied) {
    const migration = available.find((m) => m.version === row.version);
    if (!migration) throw new Error(`applied migration ${row.version} is missing`);
    if (migration.checksum !== row.checksum) throw new Error(`migration checksum drift at version ${row.version}`);
  }
}

export async function migrate(sql: FinanceSyncSql, directory: string): Promise<void> {
  const migrations = await discoverMigrations(directory);
  await sql.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(hashtextextended('finance-sync-migrations', 0))`;
    await tx`CREATE TABLE IF NOT EXISTS finance_sync_migrations (deployment_id text NOT NULL, version integer NOT NULL, name text NOT NULL, checksum char(64) NOT NULL, applied_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (deployment_id, version))`;
    const deploymentId = process.env.FINANCE_SYNC_DEPLOYMENT_ID;
    if (!deploymentId) throw new Error("FINANCE_SYNC_DEPLOYMENT_ID is required");
    const applied = await tx<AppliedMigration[]>`SELECT version, checksum FROM finance_sync_migrations WHERE deployment_id = ${deploymentId} ORDER BY version`;
    verifyAppliedMigrations(applied, migrations);
    for (const migration of migrations.filter((m) => !applied.some((a) => a.version === m.version))) {
      await tx.unsafe(migration.sql);
      await tx`INSERT INTO finance_sync_migrations (deployment_id, version, name, checksum) VALUES (${deploymentId}, ${migration.version}, ${migration.name}, ${migration.checksum})`;
    }
  });
}
