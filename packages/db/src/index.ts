import { Pool, type QueryResultRow } from "pg";
import { env } from "@dct/config";

export const pool = new Pool({ connectionString: env.DATABASE_URL });

export async function query<T extends QueryResultRow>(
  sql: string,
  params: unknown[] = []
): Promise<T[]> {
  const result = await pool.query<T>(sql, params);
  return result.rows;
}

export async function closePool(): Promise<void> {
  await pool.end();
}
