import { describe, it, expect } from 'vitest';
import { RawLogEventSchema, toRow, type RawLogEvent } from './log-event.js';

const validEvent: RawLogEvent = {
  timestamp: '2026-07-31T12:34:56.789Z',
  level: 'ERROR',
  service: 'payment-gateway',
  host: 'host-03',
  traceId: '7f3d9c2a-1b4e-4a8f-9c1d-2e5b6a7c8d90',
  statusCode: 503,
  latencyMs: 1420,
  message: 'upstream timeout',
};

describe('RawLogEventSchema', () => {
  it('accepts a well-formed event', () => {
    const result = RawLogEventSchema.safeParse(validEvent);
    expect(result.success).toBe(true);
  });

  it.each([ 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL' ])('accepts level %s', (level) => {
    expect(RawLogEventSchema.safeParse({ ...validEvent, level }).success).toBe(true);
  });

  it.each([
    [ 'a non-ISO timestamp', { timestamp: '31/07/2026 12:00' } ],
    [ 'an unknown level', { level: 'TRACE' } ],
    [ 'a status code below 100', { statusCode: 99 } ],
    [ 'a status code above 599', { statusCode: 600 } ],
    [ 'a fractional status code', { statusCode: 200.5 } ],
    [ 'a negative latency', { latencyMs: -1 } ],
    [ 'a fractional latency', { latencyMs: 12.5 } ],
    [ 'an empty service', { service: '' } ],
    [ 'an empty host', { host: '' } ],
    [ 'an empty traceId', { traceId: '' } ],
    [ 'a numeric latency sent as a string', { latencyMs: '120' } ],
  ])('rejects %s', (_label, override) => {
    const result = RawLogEventSchema.safeParse({ ...validEvent, ...override });
    expect(result.success).toBe(false);
  });

  it.each([ 'timestamp', 'level', 'service', 'host', 'traceId', 'statusCode', 'latencyMs', 'message' ])(
    'rejects an event missing %s',
    (field) => {
      const incomplete: Record<string, unknown> = { ...validEvent };
      delete incomplete[field];
      expect(RawLogEventSchema.safeParse(incomplete).success).toBe(false);
    },
  );

  it('accepts an empty message, which carries no minimum length', () => {
    expect(RawLogEventSchema.safeParse({ ...validEvent, message: '' }).success).toBe(true);
  });
});

describe('toRow', () => {
  it('maps camelCase event fields onto the snake_case ClickHouse columns', () => {
    expect(toRow(validEvent)).toEqual({
      timestamp: '2026-07-31T12:34:56.789Z',
      level: 'ERROR',
      service: 'payment-gateway',
      host: 'host-03',
      trace_id: '7f3d9c2a-1b4e-4a8f-9c1d-2e5b6a7c8d90',
      status_code: 503,
      latency_ms: 1420,
      message: 'upstream timeout',
    });
  });

  it('emits exactly the columns the logs table declares, and no extras', () => {
    expect(Object.keys(toRow(validEvent)).sort()).toEqual([
      'host', 'latency_ms', 'level', 'message', 'service', 'status_code', 'timestamp', 'trace_id',
    ]);
  });
});
