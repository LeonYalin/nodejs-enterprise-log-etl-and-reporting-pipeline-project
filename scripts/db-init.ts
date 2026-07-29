import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '@clickhouse/client';
import { clickhouseClient } from '../src/lib/clickhouse.js';
import { config } from '../src/config/index.js';
import { logger } from '../src/lib/logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const initDir = path.resolve(__dirname, '../clickhouse/init');

// clickhouseClient is scoped to config.CLICKHOUSE_DB, which doesn't exist yet on a
// fresh instance -- the ClickHouse HTTP interface rejects every request (including
// the CREATE DATABASE itself) if its `database` query param doesn't exist. This
// second client stays scoped to the always-present "default" database, and is used
// only for the CREATE DATABASE statement.
const bootstrapClient = createClient({
  url: config.CLICKHOUSE_URL,
  username: config.CLICKHOUSE_USER,
  password: config.CLICKHOUSE_PASSWORD,
});

function splitStatements(sql: string): string[] {
  return sql.split(';').map((s) => s.trim()).filter(Boolean);
}

async function applyFile(file: string): Promise<void> {
  const sql = readFileSync(path.join(initDir, file), 'utf8');
  const statements = splitStatements(sql);

  for (const statement of statements) {
    if (/^CREATE DATABASE/i.test(statement)) {
      await bootstrapClient.command({ query: statement });
    } else if (/^USE\s/i.test(statement)) {
      // No-op: clickhouseClient already targets config.CLICKHOUSE_DB.
      continue;
    } else {
      await clickhouseClient.command({ query: statement });
    }
  }

  logger.info({ file, statements: statements.length }, 'Applied ClickHouse init script');
}

async function verify(): Promise<void> {
  const expected = [ 'logs', 'logs_1m', 'logs_1m_mv' ];
  const resultSet = await clickhouseClient.query({
    query: 'SELECT name FROM system.tables WHERE database = {db:String} AND name IN {names:Array(String)}',
    query_params: { db: config.CLICKHOUSE_DB, names: expected },
    format: 'JSONEachRow',
  });
  const rows = await resultSet.json<{ name: string }>();
  const found = new Set(rows.map((row) => row.name));
  const missing = expected.filter((name) => !found.has(name));

  if (missing.length > 0) {
    throw new Error(`ClickHouse schema verification failed, missing: ${missing.join(', ')}`);
  }

  logger.info({ tables: expected }, 'ClickHouse schema verified');
}

async function main(): Promise<void> {
  const files = readdirSync(initDir).filter((f) => f.endsWith('.sql')).sort();

  for (const file of files) {
    await applyFile(file);
  }

  await verify();
}

main()
  .then(async () => {
    await bootstrapClient.close();
    await clickhouseClient.close();
    process.exit(0);
  })
  .catch(async (err) => {
    logger.error({ err }, 'ClickHouse schema initialization failed');
    await bootstrapClient.close();
    await clickhouseClient.close();
    process.exit(1);
  });
