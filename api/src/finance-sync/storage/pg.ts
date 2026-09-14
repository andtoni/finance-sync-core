import postgres, { type Sql } from "postgres";

export type FinanceSyncSql = Sql<Record<string, never>>;

export function connectFinanceSync(databaseUrl = process.env.FINANCE_SYNC_DATABASE_URL): FinanceSyncSql {
  if (!databaseUrl) throw new Error("FINANCE_SYNC_DATABASE_URL is required");
  return postgres(databaseUrl, { max: 10, prepare: false });
}

/** PostgreSQL bigint is returned as text by postgres.js. The bridge uses JSON
 * numbers (receiver i64), so refuse values that JavaScript cannot encode exactly. */
export function observationRevisionNumber(value: unknown): number {
  if (typeof value !== 'number' && (typeof value !== 'string' || !/^[1-9][0-9]*$/.test(value))) {
    throw new Error('invalid observation revision')
  }
  const revision = Number(value)
  if (!Number.isSafeInteger(revision) || revision <= 0) throw new Error('observation revision exceeds safe integer range')
  return revision
}
