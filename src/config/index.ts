import { z } from "zod";

const configSchema = z.object({
  // Kafka
  KAFKA_BROKERS: z.string().transform(val => val.split(",")),
  KAFKA_TOPIC: z.string().default("log-events"),
  KAFKA_DLQ_TOPIC: z.string().default("log-events-dlq"),
  KAFKA_GROUP_ID: z.string().default("log-pipeline-consumer"),

  // ClickHouse
  CLICKHOUSE_URL: z.string().url().default("http://localhost:8123"),
  CLICKHOUSE_DB: z.string().default("default"),
  CLICKHOUSE_USER: z.string().default("default"),
  CLICKHOUSE_PASSWORD: z.string().default(""),

  // Pipeline Metrics
  BATCH_SIZE: z.coerce.number().int().positive().default(5000),
  FLUSH_INTERVAL_MS: z.coerce.number().int().positive().default(1000),
  PRODUCER_RATE: z.coerce.number().int().positive().default(10000),

  // Server Ports
  API_PORT: z.coerce.number().int().positive().default(3000),
  PRODUCER_METRICS_PORT: z.coerce.number().int().positive().default(9101),
  CONSUMER_METRICS_PORT: z.coerce.number().int().positive().default(9102),

  // App env
  NODE_ENV: z.enum([ "development", "production", "test" ]).default("development")
});

export type Config = z.infer<typeof configSchema>;

/**
 * Parses and validates environment variables into typed config.
 *
 * Throws rather than calling process.exit() so this stays a pure function: the
 * entrypoints already catch, log and exit on startup failure, so fail-fast
 * behaviour is unchanged -- but tests can assert on validation without tearing
 * down the process.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const result = configSchema.safeParse(env);

  if (!result.success) {
    throw new Error(
      `Invalid environment configuration:\n${JSON.stringify(z.treeifyError(result.error), null, 2)}`,
    );
  }

  return result.data;
}

/**
 * Process-wide config singleton, resolved once at import.
 *
 * Deliberately a singleton: config is ambient process state read once from the
 * environment at boot. Only I/O clients get injected -- see src/lib/.
 */
export const config = loadConfig();
