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

const result = configSchema.safeParse(process.env);

if (!result.success) {
  console.error('❌ Invalid environment configuration:', JSON.stringify(result.error.format(), null, 2));
  process.exit(1);
}

export const config = result.data;
export type Config = z.infer<typeof configSchema>;
