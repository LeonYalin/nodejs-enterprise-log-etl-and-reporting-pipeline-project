import { faker } from '@faker-js/faker';
import type { RawLogEvent } from '../domain/log-event.js';

/** 20 ticks per second: small enough to smooth the send rate, large enough to stay cheap. */
export const TICK_INTERVAL_MS = 50;

/** Cap on a single sendBatch call, so one tick never ships one giant batch. */
export const SEND_CHUNK_SIZE = 1000;

// Share of each tick deliberately corrupted to exercise the consumer's DLQ path.
const RAW_GARBAGE_RATIO = 0.01;
const INVALID_SCHEMA_RATIO = 0.02; // cumulative: 0.01-0.02 is the schema-violating slice

export const RAW_GARBAGE_PAYLOAD = 'MALFORMED_RAW_STREAM_GARBAGE_CRASH_TEST_12345';
export const INVALID_SCHEMA_PAYLOAD = { badField: 'missing required root elements' };

/** Source of randomness. Defaults to Math.random; injectable so a caller can pin a branch. */
export type Random = () => number;

/** How many messages one tick must emit to sustain the target rate. */
export function messagesPerTick(ratePerSecond: number, tickIntervalMs = TICK_INTERVAL_MS): number {
  return Math.ceil(ratePerSecond / (1000 / tickIntervalMs));
}

/**
 * Generates a mock log event matching RawLogEventSchema, with a realistic
 * status/latency distribution (mostly INFO, some WARN/ERROR, occasional 5xx).
 */
export function createMockLog(random: Random = Math.random): RawLogEvent {
  const roll = random();
  let level: RawLogEvent['level'] = 'INFO';
  let statusCode = 200;
  let latencyMs = faker.number.int({ min: 5, max: 120 });

  if (roll > 0.85 && roll <= 0.95) {
    level = 'WARN';
    statusCode = faker.helpers.arrayElement([ 301, 302, 404 ]);
    latencyMs = faker.number.int({ min: 100, max: 500 });
  } else if (roll > 0.95) {
    level = 'ERROR';
    statusCode = faker.helpers.arrayElement([ 500, 502, 503, 504 ]);
    latencyMs = faker.number.int({ min: 800, max: 3500 });
  }

  return {
    timestamp: new Date().toISOString(),
    level,
    service: faker.helpers.arrayElement([ 'auth-service', 'payment-gateway', 'api-router', 'inventory-manager' ]),
    host: `host-${faker.number.int({ min: 1, max: 10 }).toString().padStart(2, '0')}`,
    traceId: faker.string.uuid(),
    statusCode,
    latencyMs,
    message: faker.hacker.phrase(),
  };
}

/**
 * Builds one tick's worth of Kafka messages, mixing in a small percentage of
 * intentionally malformed payloads to exercise the consumer's DLQ path.
 */
export function buildTickMessages(count: number, random: Random = Math.random): { value: string }[] {
  const messages: { value: string }[] = [];

  for (let i = 0; i < count; i++) {
    const errorRoll = random();

    if (errorRoll < RAW_GARBAGE_RATIO) {
      messages.push({ value: RAW_GARBAGE_PAYLOAD });
    } else if (errorRoll < INVALID_SCHEMA_RATIO) {
      // Syntactically valid JSON that fails schema validation.
      messages.push({ value: JSON.stringify(INVALID_SCHEMA_PAYLOAD) });
    } else {
      messages.push({ value: JSON.stringify(createMockLog(random)) });
    }
  }

  return messages;
}
