import { z } from "zod";

/**
 * Runtime validation schema for incoming Kafka events.
 * Validates what the producer emits and what the consumer receives
 */
export const RawLogEventSchema = z.object({
  ts: z.iso.datetime({ message: "Must be a valid ISO 8601 UTC timestamp" }),
  level: z.enum([ 'DEBUG', 'INFO', "WARN", "ERROR", "FATAL" ]),
  service: z.string().min(1),
  host: z.string().min(1),
  traceId: z.string().min(1),
  statusCode: z.number().int().min(100).max(599),
  latencyMs: z.number().int().nonnegative(),
  message: z.string(),
});

export type RawLogEvent = z.infer<typeof RawLogEventSchema>;

/**
 * Inferred TypeScript interface for the ClickHouse schema.
 * Columns use snake_case and match standard ClickHouse-compatible JS types.
 */
export interface ClickHouseLogRow {
  ts: string; // DateTime64(3) — column name matches clickhouse/init/01_schema.sql (ORDER BY (service, level, ts))
  level: string; // LowCardinality(String)
  service: string; // LowCardinality(String)
  host: string; // LowCardinality(String)
  trace_id: string; // String
  status_code: number; // UInt16
  latency_ms: number; // UInt32
  message: string; // String
}

/**
 * Converts a validated RawLogEvent into an optimized ClickHouse row structure.
 */
export function toRow(event: RawLogEvent): ClickHouseLogRow {
  return {
    ts: event.ts,
    level: event.level,
    service: event.service,
    host: event.host,
    trace_id: event.traceId,
    status_code: event.statusCode,
    latency_ms: event.latencyMs,
    message: event.message,
  };
}
