import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ClickHouseClient } from '@clickhouse/client';
import { logger } from './logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** Directory holding the canonical schema/MV definitions applied to every environment. */
export const INIT_DIR = path.resolve(__dirname, '../../clickhouse/init');

export const EXPECTED_TABLES = [ 'logs', 'logs_1m', 'logs_1m_mv' ];

export interface SchemaInitDeps {
  /** Scoped to the target database (config.CLICKHOUSE_DB). */
  client: ClickHouseClient;
  /**
   * Scoped to the always-present "default" database. Needed because the ClickHouse
   * HTTP interface rejects every request -- including the CREATE DATABASE itself --
   * if its `database` query param names a database that doesn't exist yet.
   */
  bootstrapClient: ClickHouseClient;
  database: string;
}

function splitStatements(sql: string): string[] {
  return sql.split(';').map((s) => s.trim()).filter(Boolean);
}

async function applyFile(file: string, { client, bootstrapClient }: SchemaInitDeps): Promise<void> {
  const sql = readFileSync(path.join(INIT_DIR, file), 'utf8');
  const statements = splitStatements(sql);

  for (const statement of statements) {
    if (/^CREATE DATABASE/i.test(statement)) {
      await bootstrapClient.command({ query: statement });
    } else if (/^USE\s/i.test(statement)) {
      // No-op: client already targets the configured database.
      continue;
    } else {
      await client.command({ query: statement });
    }
  }

  logger.info({ file, statements: statements.length }, 'Applied ClickHouse init script');
}

async function verify({ client, database }: SchemaInitDeps): Promise<void> {
  const resultSet = await client.query({
    query: 'SELECT name FROM system.tables WHERE database = {db:String} AND name IN {names:Array(String)}',
    query_params: { db: database, names: EXPECTED_TABLES },
    format: 'JSONEachRow',
  });
  const rows = await resultSet.json<{ name: string }>();
  const found = new Set(rows.map((row) => row.name));
  const missing = EXPECTED_TABLES.filter((name) => !found.has(name));

  if (missing.length > 0) {
    throw new Error(`ClickHouse schema verification failed, missing: ${missing.join(', ')}`);
  }

  logger.info({ tables: EXPECTED_TABLES }, 'ClickHouse schema verified');
}

/**
 * Applies every clickhouse/init/*.sql file in order, then verifies the expected
 * tables exist. Shared by the db:init script and the integration suite so the
 * schema is defined in exactly one place.
 */
export async function applyInitScripts(deps: SchemaInitDeps): Promise<void> {
  const files = readdirSync(INIT_DIR).filter((f) => f.endsWith('.sql')).sort();

  for (const file of files) {
    await applyFile(file, deps);
  }

  await verify(deps);
}
