import { defineConfig } from 'vitest/config';

// src/config validates at import time and KAFKA_BROKERS has no default, so these
// must exist before any module under test loads. NODE_ENV=test also silences pino
// (see src/lib/logger.ts).
const baseEnv = {
  NODE_ENV: 'test',
  KAFKA_BROKERS: 'localhost:9092',
  KAFKA_TOPIC: 'log-events',
  KAFKA_DLQ_TOPIC: 'log-events-dlq',
  CLICKHOUSE_URL: 'http://localhost:8123',
  CLICKHOUSE_DB: 'log_pipeline',
};

const unitEnv = {
  ...baseEnv,
  // Small values so buffer tests read clearly instead of building 5000 rows.
  BATCH_SIZE: '10',
  FLUSH_INTERVAL_MS: '100',
};

// globalSetup runs in Vitest's main process, where the projects' `env` blocks do
// not apply -- but it imports src/lib/schema-init, which pulls in the config
// singleton. This config file is evaluated first, so seed the vars here too.
// Integration tests build their own clients from the injected container URLs, so
// these placeholder values are never actually connected to.
for (const [ key, value ] of Object.entries(baseEnv)) {
  process.env[key] ??= value;
}

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          environment: 'node',
          include: [ 'src/**/*.test.ts' ],
          env: unitEnv,
        },
      },
      {
        test: {
          name: 'integration',
          environment: 'node',
          include: [ 'tests/integration/**/*.test.ts' ],
          globalSetup: [ './tests/integration/globalSetup.ts' ],
          // Containers are slow to boot and the suites share one Kafka/ClickHouse
          // pair, so run files serially with generous timeouts.
          testTimeout: 120_000,
          hookTimeout: 180_000,
          fileParallelism: false,
          env: baseEnv,
        },
      },
    ],
  },
});
