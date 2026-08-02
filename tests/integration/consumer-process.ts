import { spawn, type ChildProcess } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getFreePort, waitFor, type ClickHouseConnection } from './helpers.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const TSX = path.join(repoRoot, 'node_modules/.bin/tsx');
const CONSUMER_ENTRYPOINT = path.join(repoRoot, 'src/consumer/index.ts');

export interface ConsumerProcessOptions {
  brokers: string;
  clickhouse: ClickHouseConnection;
  topic: string;
  dlqTopic: string;
  groupId: string;
  batchSize: number;
  flushIntervalMs: number;
}

/**
 * Runs the real shipped consumer entrypoint as a child process, so the suite
 * exercises the actual process -- composition root, signal handling and all --
 * rather than re-implementing its wiring in the test.
 */
export async function startConsumerProcess(options: ConsumerProcessOptions) {
  const metricsPort = await getFreePort();
  const output: string[] = [];

  const child: ChildProcess = spawn(TSX, [ CONSUMER_ENTRYPOINT ], {
    cwd: repoRoot,
    env: {
      ...process.env,
      // NODE_ENV=production keeps pino on plain JSON stdout (no pino-pretty) and
      // avoids the 'test' level, which silences logging entirely.
      NODE_ENV: 'production',
      KAFKA_BROKERS: options.brokers,
      KAFKA_TOPIC: options.topic,
      KAFKA_DLQ_TOPIC: options.dlqTopic,
      KAFKA_GROUP_ID: options.groupId,
      CLICKHOUSE_URL: options.clickhouse.url,
      CLICKHOUSE_DB: options.clickhouse.database,
      CLICKHOUSE_USER: options.clickhouse.username,
      CLICKHOUSE_PASSWORD: options.clickhouse.password,
      BATCH_SIZE: String(options.batchSize),
      FLUSH_INTERVAL_MS: String(options.flushIntervalMs),
      CONSUMER_METRICS_PORT: String(metricsPort),
    },
    stdio: [ 'ignore', 'pipe', 'pipe' ],
  });

  child.stdout?.on('data', (chunk) => output.push(String(chunk)));
  child.stderr?.on('data', (chunk) => output.push(String(chunk)));

  const exited = new Promise<number | null>((resolve) => {
    child.on('exit', (code) => resolve(code));
  });

  /** Scrapes a single counter/gauge value off the consumer's own /metrics endpoint. */
  async function readMetric(name: string): Promise<number> {
    const response = await fetch(`http://localhost:${metricsPort}/metrics`);
    const body = await response.text();
    let total = 0;
    for (const line of body.split('\n')) {
      if (line.startsWith('#') || !line.startsWith(name)) continue;
      const value = Number(line.slice(line.lastIndexOf(' ') + 1));
      if (!Number.isNaN(value)) total += value;
    }
    return total;
  }

  return {
    child,
    metricsPort,
    readMetric,
    logs: () => output.join(''),
    async stop(signal: NodeJS.Signals = 'SIGTERM'): Promise<number | null> {
      if (child.exitCode !== null || child.signalCode !== null) return child.exitCode;
      child.kill(signal);
      return exited;
    },
    async kill(): Promise<void> {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
        await exited;
      }
    },
    waitForMetric(name: string, predicate: (value: number) => boolean, description: string) {
      return waitFor(description, async () => predicate(await readMetric(name)) || undefined);
    },
  };
}

export type ConsumerProcess = Awaited<ReturnType<typeof startConsumerProcess>>;
