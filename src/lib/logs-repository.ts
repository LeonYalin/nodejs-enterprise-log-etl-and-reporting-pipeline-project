import type { ClickHouseClient } from '@clickhouse/client';
import type { ClickHouseLogRow } from '../domain/log-event.js';

const TABLE = 'logs';

/**
 * Owns the pipeline's only ClickHouse write path.
 *
 * Consumers depend on this instead of the raw @clickhouse/client so the write is
 * expressed once (bulk JSONEachRow, never row-by-row) and callers can be tested
 * against a plain object of the same shape -- no module mocking required.
 */
export function createLogsRepository(client: ClickHouseClient) {
  return {
    async insertMany(rows: ClickHouseLogRow[]): Promise<void> {
      await client.insert({ table: TABLE, values: rows, format: 'JSONEachRow' });
    },
  };
}

export type LogsRepository = ReturnType<typeof createLogsRepository>;
