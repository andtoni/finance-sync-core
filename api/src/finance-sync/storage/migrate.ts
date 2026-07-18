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
    // Schema migrations are database-global. Preserve the former deployment-scoped
    // ledger as immutable convergence evidence, but promote each unambiguous version
    // into the canonical global ledger before applying anything new.
    await tx`CREATE TABLE IF NOT EXISTS finance_sync_migrations (deployment_id text NOT NULL, version integer NOT NULL, name text NOT NULL, checksum char(64) NOT NULL, applied_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (deployment_id, version))`;
    await tx`CREATE TABLE IF NOT EXISTS finance_sync_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, checksum char(64) NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`;
    const conflicts = await tx<{ version: number }[]>`
      SELECT version FROM finance_sync_migrations GROUP BY version HAVING count(DISTINCT (name, checksum)) > 1
      UNION
      SELECT legacy.version
      FROM finance_sync_migrations legacy
      JOIN finance_sync_schema_migrations canonical USING (version)
      WHERE legacy.name <> canonical.name OR legacy.checksum <> canonical.checksum
      LIMIT 1
    `;
    if (conflicts.length > 0) throw new Error(`legacy migration checksum conflict at version ${conflicts[0].version}`);
    await tx`INSERT INTO finance_sync_schema_migrations (version, name, checksum, applied_at) SELECT DISTINCT ON (version) version, name, checksum, applied_at FROM finance_sync_migrations ORDER BY version, applied_at ASC ON CONFLICT (version) DO NOTHING`;
    const applied = await tx<AppliedMigration[]>`SELECT version, checksum FROM finance_sync_schema_migrations ORDER BY version`;
    verifyAppliedMigrations(applied, migrations);
    for (const migration of migrations.filter((m) => !applied.some((a) => a.version === m.version))) {
      await tx.unsafe(migration.sql);
      await tx`INSERT INTO finance_sync_schema_migrations (version, name, checksum) VALUES (${migration.version}, ${migration.name}, ${migration.checksum})`;
    }
  });
}
