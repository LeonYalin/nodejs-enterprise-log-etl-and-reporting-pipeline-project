import { describe, it, expect } from 'vitest';
import {
  createMockLog,
  buildTickMessages,
  messagesPerTick,
  RAW_GARBAGE_PAYLOAD,
  INVALID_SCHEMA_PAYLOAD,
  TICK_INTERVAL_MS,
} from './generator.js';
import { RawLogEventSchema } from '../domain/log-event.js';
import { classifyMessage } from '../consumer/transform.js';

/** Pins every random branch to a fixed roll. */
const fixed = (value: number) => () => value;

describe('messagesPerTick', () => {
  it('splits the target rate across 20 ticks per second', () => {
    expect(messagesPerTick(10_000, 50)).toBe(500);
  });

  it('sustains the rate for the default tick interval', () => {
    const perTick = messagesPerTick(10_000);
    const ticksPerSecond = 1000 / TICK_INTERVAL_MS;

    expect(perTick * ticksPerSecond).toBeGreaterThanOrEqual(10_000);
  });

  it('rounds up so the target rate is never undershot', () => {
    // 1001/sec over 20 ticks is 50.05 -> 51, not 50.
    expect(messagesPerTick(1001, 50)).toBe(51);
  });

  it.each([ [ 1000, 50 ], [ 20, 50 ], [ 500, 100 ] ])(
    'never emits fewer than the rate demands (%i/sec, %ims tick)',
    (rate, tick) => {
      expect(messagesPerTick(rate, tick) * (1000 / tick)).toBeGreaterThanOrEqual(rate);
    },
  );
});

describe('createMockLog', () => {
  it('always produces an event the consumer will accept', () => {
    // The producer/consumer wire contract: anything generated here must satisfy
    // the schema the consumer validates against, or it silently lands in the DLQ.
    for (let i = 0; i < 200; i++) {
      const result = RawLogEventSchema.safeParse(createMockLog());
      expect(result.success).toBe(true);
    }
  });

  it('emits INFO/200 on a low roll', () => {
    const log = createMockLog(fixed(0.5));

    expect(log.level).toBe('INFO');
    expect(log.statusCode).toBe(200);
    expect(log.latencyMs).toBeGreaterThanOrEqual(5);
    expect(log.latencyMs).toBeLessThanOrEqual(120);
  });

  it('emits WARN with a redirect/not-found status in the 0.85-0.95 band', () => {
    const log = createMockLog(fixed(0.9));

    expect(log.level).toBe('WARN');
    expect([ 301, 302, 404 ]).toContain(log.statusCode);
    expect(log.latencyMs).toBeGreaterThanOrEqual(100);
    expect(log.latencyMs).toBeLessThanOrEqual(500);
  });

  it('emits ERROR with a 5xx and high latency above 0.95', () => {
    const log = createMockLog(fixed(0.99));

    expect(log.level).toBe('ERROR');
    expect([ 500, 502, 503, 504 ]).toContain(log.statusCode);
    expect(log.latencyMs).toBeGreaterThanOrEqual(800);
    expect(log.latencyMs).toBeLessThanOrEqual(3500);
  });

  it('emits an ISO 8601 timestamp', () => {
    expect(createMockLog().timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  });

  it('keeps 5xx statuses exclusive to ERROR, so MV error_count stays meaningful', () => {
    for (let i = 0; i < 200; i++) {
      const log = createMockLog();
      if (log.statusCode >= 500) expect(log.level).toBe('ERROR');
    }
  });
});

describe('buildTickMessages', () => {
  it('emits exactly the requested count', () => {
    expect(buildTickMessages(37)).toHaveLength(37);
  });

  it('returns nothing for a zero count', () => {
    expect(buildTickMessages(0)).toEqual([]);
  });

  it('emits raw garbage below the 1% threshold', () => {
    const messages = buildTickMessages(5, fixed(0.005));

    expect(messages.every((m) => m.value === RAW_GARBAGE_PAYLOAD)).toBe(true);
  });

  it('emits schema-violating JSON in the 1-2% band', () => {
    const messages = buildTickMessages(5, fixed(0.015));

    expect(messages.every((m) => m.value === JSON.stringify(INVALID_SCHEMA_PAYLOAD))).toBe(true);
    // Valid JSON, invalid shape -- the distinction the DLQ reasons depend on.
    expect(() => JSON.parse(messages[0]!.value)).not.toThrow();
  });

  it('emits valid events above the 2% threshold', () => {
    const messages = buildTickMessages(5, fixed(0.5));

    for (const message of messages) {
      expect(RawLogEventSchema.safeParse(JSON.parse(message.value)).success).toBe(true);
    }
  });

  it('produces a payload the consumer classifies as intended, for each branch', () => {
    // Closes the loop end-to-end at the unit level: what the producer emits is
    // exactly what classifyMessage sorts into rows vs. DLQ reasons.
    const toBuffer = (v: string) => Buffer.from(v);

    expect(classifyMessage(toBuffer(buildTickMessages(1, fixed(0.005))[0]!.value)))
      .toMatchObject({ kind: 'invalid', reason: 'INVALID_JSON_STRING_FORMAT' });

    const schemaViolation = classifyMessage(toBuffer(buildTickMessages(1, fixed(0.015))[0]!.value));
    expect(schemaViolation.kind).toBe('invalid');
    if (schemaViolation.kind === 'invalid') {
      expect(schemaViolation.reason).toMatch(/^SCHEMA_VIOLATION: /);
    }

    expect(classifyMessage(toBuffer(buildTickMessages(1, fixed(0.5))[0]!.value)))
      .toMatchObject({ kind: 'valid' });
  });

  it('keeps the malformed share near 2% over a large tick', () => {
    const messages = buildTickMessages(5000);
    const malformed = messages.filter(
      (m) => m.value === RAW_GARBAGE_PAYLOAD || m.value === JSON.stringify(INVALID_SCHEMA_PAYLOAD),
    );

    // Generous band: this guards the ratio constants, not the RNG.
    expect(malformed.length / messages.length).toBeGreaterThan(0.005);
    expect(malformed.length / messages.length).toBeLessThan(0.05);
  });
});
