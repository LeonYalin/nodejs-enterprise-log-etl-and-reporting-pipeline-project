import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createBatchBuffer } from './batch-buffer.js';
import type { ClickHouseLogRow } from '../domain/log-event.js';

function row(i: number): ClickHouseLogRow {
  return {
    timestamp: '2026-07-31T12:00:00.000Z',
    level: 'INFO',
    service: 'auth-service',
    host: 'host-01',
    trace_id: `trace-${i}`,
    status_code: 200,
    latency_ms: i,
    message: `msg-${i}`,
  };
}

const rows = (n: number) => Array.from({ length: n }, (_, i) => row(i));

/**
 * Stand-in for the logs repository. Hand-written rather than mocked so it can do
 * what a real ClickHouse cannot: fail on demand, and hang mid-insert.
 */
function createFakeLogs() {
  const batches: ClickHouseLogRow[][] = [];
  let failures = 0;
  let gate: { promise: Promise<void>; release: () => void } | null = null;

  return {
    batches,
    /** Make the next `n` inserts reject. */
    failNext(n: number) { failures = n; },
    /** Make inserts hang until release() is called. */
    block() {
      let release!: () => void;
      const promise = new Promise<void>((resolve) => { release = resolve; });
      gate = { promise, release };
      return () => gate!.release();
    },
    async insertMany(batch: ClickHouseLogRow[]): Promise<void> {
      if (gate) await gate.promise;
      if (failures > 0) {
        failures--;
        throw new Error('ClickHouse unavailable');
      }
      batches.push(batch);
    },
  };
}

// Lets queued microtasks settle without advancing wall-clock time.
const settle = () => vi.advanceTimersByTimeAsync(0);

describe('createBatchBuffer', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('flushes as soon as the row count reaches batchSize', async () => {
    const logs = createFakeLogs();
    const buffer = createBatchBuffer({ logs, batchSize: 10, flushIntervalMs: 1000 });

    buffer.add(rows(10), 0, '9');
    await settle();

    expect(logs.batches).toHaveLength(1);
    expect(logs.batches[0]).toHaveLength(10);
    expect(buffer.size()).toBe(0);
  });

  it('flushes on the interval timer when it stays below batchSize', async () => {
    const logs = createFakeLogs();
    const buffer = createBatchBuffer({ logs, batchSize: 10, flushIntervalMs: 1000 });

    buffer.add(rows(3), 0, '2');
    expect(buffer.size()).toBe(3);

    // Not yet -- the size threshold was never reached.
    await vi.advanceTimersByTimeAsync(999);
    expect(logs.batches).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(1);
    expect(logs.batches).toHaveLength(1);
    expect(logs.batches[0]).toHaveLength(3);
    expect(buffer.size()).toBe(0);
  });

  it('coalesces rows from several adds into a single insert', async () => {
    const logs = createFakeLogs();
    const buffer = createBatchBuffer({ logs, batchSize: 100, flushIntervalMs: 1000 });

    buffer.add(rows(2), 0, '1');
    buffer.add(rows(3), 0, '4');
    await vi.advanceTimersByTimeAsync(1000);

    expect(logs.batches).toHaveLength(1);
    expect(logs.batches[0]).toHaveLength(5);
  });

  it('checkpoints a row-free batch without inserting anything', async () => {
    // A batch of nothing but malformed messages still has to carry its offset
    // through the flush path, or those offsets are never committed and the
    // messages are redelivered forever.
    const logs = createFakeLogs();
    const seen: Map<number, string>[] = [];
    const buffer = createBatchBuffer({
      logs,
      batchSize: 10,
      flushIntervalMs: 1000,
      onFlush: async (offsets) => { seen.push(offsets); },
    });

    buffer.add([], 0, '7');
    expect(buffer.size()).toBe(0);

    await vi.advanceTimersByTimeAsync(1000);

    expect(logs.batches).toHaveLength(0);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.get(0)).toBe('7');
  });

  it('carries offset-only checkpoints alongside rows in one flush', async () => {
    const logs = createFakeLogs();
    const seen: Map<number, string>[] = [];
    const buffer = createBatchBuffer({
      logs,
      batchSize: 100,
      flushIntervalMs: 1000,
      onFlush: async (offsets) => { seen.push(offsets); },
    });

    buffer.add(rows(2), 0, '4');
    buffer.add([], 0, '9');
    await vi.advanceTimersByTimeAsync(1000);

    expect(logs.batches).toEqual([ expect.objectContaining({ length: 2 }) ]);
    expect(seen[0]!.get(0)).toBe('9');
  });

  it('does not start a second insert while one is in flight', async () => {
    const logs = createFakeLogs();
    const release = logs.block();
    const buffer = createBatchBuffer({ logs, batchSize: 10, flushIntervalMs: 1000 });

    buffer.add(rows(10), 0, '9');
    await settle();

    // A second trigger during the in-flight insert must be a no-op.
    buffer.add(rows(10), 0, '19');
    void buffer.forceFlush();
    await settle();

    release();
    await settle();

    // The rows added mid-flight stay buffered for the next cycle.
    expect(logs.batches).toHaveLength(1);
    expect(buffer.size()).toBe(10);
  });

  it('restores rows to the buffer when the insert fails, and never throws', async () => {
    const logs = createFakeLogs();
    logs.failNext(1);
    const buffer = createBatchBuffer({ logs, batchSize: 5, flushIntervalMs: 1000 });

    buffer.add(rows(5), 0, '4');
    await settle();

    // Nothing landed, but the rows are still held for a retry.
    expect(logs.batches).toHaveLength(0);
    expect(buffer.size()).toBe(5);

    await buffer.forceFlush();

    expect(logs.batches).toHaveLength(1);
    expect(logs.batches[0]).toHaveLength(5);
    expect(buffer.size()).toBe(0);
  });

  it('surfaces no unhandled rejection when a size-triggered flush fails', async () => {
    const logs = createFakeLogs();
    logs.failNext(1);
    const buffer = createBatchBuffer({ logs, batchSize: 2, flushIntervalMs: 1000 });

    // add() fires forceFlush() fire-and-forget; a throw here would crash the consumer.
    expect(() => buffer.add(rows(2), 0, '1')).not.toThrow();
    await settle();

    expect(buffer.size()).toBe(2);
  });

  it('reports the highest offset per partition to onFlush', async () => {
    const logs = createFakeLogs();
    const seen: Map<number, string>[] = [];
    const buffer = createBatchBuffer({
      logs,
      batchSize: 100,
      flushIntervalMs: 1000,
      onFlush: async (offsets) => { seen.push(offsets); },
    });

    buffer.add(rows(1), 0, '5');
    buffer.add(rows(1), 1, '99');
    buffer.add(rows(1), 0, '12');
    // Out of order on purpose: the max must win, not the last one added.
    buffer.add(rows(1), 0, '7');
    await vi.advanceTimersByTimeAsync(1000);

    expect(seen).toHaveLength(1);
    expect(Object.fromEntries(seen[0]!)).toEqual({ 0: '12', 1: '99' });
  });

  it('compares offsets numerically, not lexicographically', async () => {
    const logs = createFakeLogs();
    const seen: Map<number, string>[] = [];
    const buffer = createBatchBuffer({
      logs,
      batchSize: 100,
      flushIntervalMs: 1000,
      onFlush: async (offsets) => { seen.push(offsets); },
    });

    // "9" > "10" as strings; BigInt comparison must pick 10.
    buffer.add(rows(1), 0, '9');
    buffer.add(rows(1), 0, '10');
    await vi.advanceTimersByTimeAsync(1000);

    expect(seen[0]!.get(0)).toBe('10');
  });

  it('does not resolve offsets when the insert fails (at-least-once)', async () => {
    const logs = createFakeLogs();
    logs.failNext(1);
    const seen: Map<number, string>[] = [];
    const buffer = createBatchBuffer({
      logs,
      batchSize: 3,
      flushIntervalMs: 1000,
      onFlush: async (offsets) => { seen.push(offsets); },
    });

    buffer.add(rows(3), 0, '2');
    await settle();

    // Committing here would drop data that ClickHouse never accepted.
    expect(seen).toHaveLength(0);

    await buffer.forceFlush();
    expect(seen).toHaveLength(1);
  });

  it('is a no-op when flushed with nothing buffered', async () => {
    const logs = createFakeLogs();
    const seen: unknown[] = [];
    const buffer = createBatchBuffer({
      logs,
      batchSize: 10,
      flushIntervalMs: 1000,
      onFlush: async (offsets) => { seen.push(offsets); },
    });

    await buffer.forceFlush();

    expect(logs.batches).toHaveLength(0);
    expect(seen).toHaveLength(0);
  });

  it('gives each instance independent state', async () => {
    const logsA = createFakeLogs();
    const logsB = createFakeLogs();
    const a = createBatchBuffer({ logs: logsA, batchSize: 10, flushIntervalMs: 1000 });
    const b = createBatchBuffer({ logs: logsB, batchSize: 10, flushIntervalMs: 1000 });

    a.add(rows(4), 0, '3');

    expect(a.size()).toBe(4);
    expect(b.size()).toBe(0);
  });
});
