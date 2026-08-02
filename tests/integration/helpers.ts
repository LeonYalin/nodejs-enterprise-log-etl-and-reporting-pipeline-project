import net from 'node:net';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

/** Reserves an ephemeral port from the OS and releases it for the caller to bind. */
export function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, () => {
      const address = server.address();
      if (typeof address === 'string' || address === null) {
        reject(new Error('Could not determine a free port'));
        return;
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });
}

export interface ClickHouseConnection {
  url: string;
  database: string;
  username: string;
  password: string;
}

/**
 * ClickHouse client for the test container. Mirrors src/lib/clickhouse.ts's
 * settings -- notably date_time_input_format, without which ISO 8601 timestamps
 * are rejected.
 */
export function createTestClickHouseClient(
  { url, database, username, password }: ClickHouseConnection,
): ClickHouseClient {
  return createClient({
    url,
    database,
    username,
    password,
    clickhouse_settings: {
      date_time_input_format: 'best_effort',
    },
  });
}

/** Polls `check` until it returns a truthy value, or the timeout elapses. */
export async function waitFor<T>(
  description: string,
  check: () => Promise<T | undefined | null | false>,
  { timeoutMs = 30_000, intervalMs = 250 } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const result = await check();
      if (result) return result;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(
    `Timed out after ${timeoutMs}ms waiting for: ${description}` +
    (lastError ? `\nLast error: ${String(lastError)}` : ''),
  );
}
