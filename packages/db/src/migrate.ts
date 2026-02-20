import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pool } from "./index.js";

async function migrate(): Promise<void> {
  const dir = path.dirname(fileURLToPath(import.meta.url));
  const migrationPath = path.resolve(dir, "../migrations/001_init.sql");
  const sql = await fs.readFile(migrationPath, "utf8");
  await pool.query(sql);
  await pool.end();
  // eslint-disable-next-line no-console
  console.log("Applied migration 001_init.sql");
}

migrate().catch(async (error) => {
  // eslint-disable-next-line no-console
  console.error("Migration failed", error);
  await pool.end();
  process.exitCode = 1;
});
