import { afterAll, describe, expect, it } from "vitest";
import postgres from "postgres";
import { migrate } from "./migrate";
import { resolve } from "node:path";

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL;
const suite = url ? describe : describe.skip;
let sql: ReturnType<typeof postgres> | undefined;

suite("PostgreSQL migrations (integration)", () => {
  it("connects and applies all migrations idempotently", async () => {
    sql = postgres(url!, { connect_timeout: 3, prepare: false });
    process.env.FINANCE_SYNC_DEPLOYMENT_ID = "integration-test";
    await sql`select 1`;
    const directory = resolve(import.meta.dirname, "../../../finance-sync-migrations");
    await migrate(sql, directory);
    await migrate(sql, directory);
    const rows = await sql`select version from finance_sync_schema_migrations order by version`;
    expect(rows.map((r) => r.version)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);

    await sql`insert into finance_sync_migrations (deployment_id, version, name, checksum) values ('conflicting-legacy', 1, 'identity', ${'0'.repeat(64)})`;
    await expect(migrate(sql, directory)).rejects.toThrow(/checksum conflict/i);
    await sql`delete from finance_sync_migrations where deployment_id = 'conflicting-legacy'`;
  });
});
afterAll(async () => { await sql?.end(); });
