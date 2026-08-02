import { RawLogEventSchema, toRow, type ClickHouseLogRow } from '../domain/log-event.js';

/**
 * Outcome of classifying a single Kafka message: either a row ready for
 * ClickHouse, or a rejection carrying the reason + raw payload for the DLQ.
 */
export type Classified =
  | { kind: 'valid'; row: ClickHouseLogRow }
  | { kind: 'invalid'; reason: string; raw: string | null };

/**
 * Decides what happens to one Kafka message: decode → parse JSON → validate
 * schema → map to the ClickHouse row shape.
 *
 * Deliberately pure and I/O-free -- it neither publishes to the DLQ nor touches
 * offsets, so the consumer's per-message policy is testable in isolation and the
 * eachBatch handler stays a thin orchestrator over it.
 */
export function classifyMessage(value: Buffer | null): Classified {
  const raw = value ? value.toString() : null;

  if (!raw) {
    return { kind: 'invalid', reason: 'EMPTY_KAFKA_PAYLOAD', raw: null };
  }

  let parsedJSON: unknown;
  try {
    parsedJSON = JSON.parse(raw);
  } catch {
    return { kind: 'invalid', reason: 'INVALID_JSON_STRING_FORMAT', raw };
  }

  const validationResult = RawLogEventSchema.safeParse(parsedJSON);
  if (!validationResult.success) {
    return { kind: 'invalid', reason: `SCHEMA_VIOLATION: ${validationResult.error.message}`, raw };
  }

  return { kind: 'valid', row: toRow(validationResult.data) };
}
