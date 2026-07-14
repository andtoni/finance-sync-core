import postgres, { type Sql } from "postgres";

export type FinanceSyncSql = Sql<Record<string, never>>;

export function connectFinanceSync(databaseUrl = process.env.FINANCE_SYNC_DATABASE_URL): FinanceSyncSql {
  if (!databaseUrl) throw new Error("FINANCE_SYNC_DATABASE_URL is required");
  return postgres(databaseUrl, { max: 10, prepare: false });
}
