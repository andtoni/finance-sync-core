import { resolve } from "node:path";
import { connectFinanceSync } from "./pg";
import { migrate } from "./migrate";

const sql = connectFinanceSync();
try {
  await migrate(sql, resolve(import.meta.dirname, "../../../finance-sync-migrations"));
} finally {
  await sql.end();
}
