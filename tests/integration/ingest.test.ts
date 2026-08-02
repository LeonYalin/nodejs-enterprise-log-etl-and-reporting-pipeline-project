import { describe, it, expect, beforeAll, afterAll, inject } from 'vitest';
import { Kafka, type Admin, type Producer } from 'kafkajs';
import type { ClickHouseClient } from '@clickhouse/client';
import type { RawLogEvent } from '../../src/domain/log-event.js';
import { createTestClickHouseClient, waitFor } from './helpers.js';
import { startConsumerProcess, type ConsumerProcess } from './consumer-process.js';

const brokers = inject('kafkaBrokers');
const connection = inject('clickhouse');

const TOPIC = 'e2e-log-events';
const DLQ_TOPIC = 'e2e-log-events-dlq';
const GROUP_ID = 'e2e-consumer';

const VALID_COUNT = 10;
const ERROR_SERVICE = 'payment-gateway';
const OK_SERVICE = 'auth-service';

const kafka = new Kafka({ clientId: 'e2e-test', brokers: [ brokers ], logLevel: 0 });

function validEvent(i: number): RawLogEvent {
  const isError = i < 4;
  return {
    timestamp: new Date().toISOString(),
    level: isError ? 'ERROR' : 'INFO',
    service: isError ? ERROR_SERVICE : OK_SERVICE,
    host: `host-0${(i % 3) + 1}`,
    traceId: `trace-${i}`,
    statusCode: isError ? 503 : 200,
    latencyMs: isError ? 900 : 25,
    message: `event-${i}`,
  };
}

let client: ClickHouseClient;
let admin: Admin;
let producer: Producer;
let consumer: ConsumerProcess;

async function countLogs(): Promise<number> {
  const result = await client.query({ query: 'SELECT count() AS c FROM logs', format: 'JSONEachRow' });
  const [ first ] = await result.json<{ c: string }>();
  return Number(first?.c ?? 0);
}

/** Reads everything currently on a topic, giving up once it goes quiet. */
async function drainTopic(topic: string, expected: number, timeoutMs = 30_000): Promise<string[]> {
  const groupId = `drain-${topic}-${Date.now()}`;
  const reader = kafka.consumer({ groupId });
  const messages: string[] = [];

  await reader.connect();
  await reader.subscribe({ topic, fromBeginning: true });
  await reader.run({
    eachMessage: async ({ message }) => {
      if (message.value) messages.push(message.value.toString());
    },
  });

  try {
    await waitFor(
      `${expected} messages on ${topic}`,
      async () => messages.length >= expected || undefined,
      { timeoutMs },
    );
  } finally {
    await reader.disconnect();
  }

  return messages;
}

// Shared across both blocks below, so neither tears the other's clients down.
beforeAll(async () => {
  client = createTestClickHouseClient(connection);
  admin = kafka.admin();
  producer = kafka.producer();
  await admin.connect();
  await producer.connect();

  await client.command({ query: 'TRUNCATE TABLE logs' });
  await client.command({ query: 'TRUNCATE TABLE logs_1m' });
});

afterAll(async () => {
  await producer?.disconnect();
  await admin?.disconnect();
  await client?.close();
});

describe('end-to-end ingest through the real consumer process', () => {
  beforeAll(async () => {
    consumer = await startConsumerProcess({
      brokers,
      clickhouse: connection,
      topic: TOPIC,
      dlqTopic: DLQ_TOPIC,
      groupId: GROUP_ID,
      batchSize: 5,
      flushIntervalMs: 300,
    });

    // The consumer subscribes with fromBeginning:false, so anything produced
    // before its partitions are assigned would be skipped. Wait for the group to
    // stabilise rather than sleeping and hoping.
    await waitFor('the consumer group to become stable', async () => {
      const { groups } = await admin.describeGroups([ GROUP_ID ]);
      const group = groups.find((g) => g.groupId === GROUP_ID);
      return (group?.state === 'Stable' && group.members.length > 0) || undefined;
    }, { timeoutMs: 60_000 });

    await producer.send({
      topic: TOPIC,
      messages: [
        ...Array.from({ length: VALID_COUNT }, (_, i) => ({ value: JSON.stringify(validEvent(i)) })),
        // Exercise both DLQ branches.
        { value: 'MALFORMED_RAW_STREAM_GARBAGE_CRASH_TEST_12345' },
        { value: JSON.stringify({ badField: 'missing required root elements' }) },
      ],
    });
  });

  afterAll(async () => {
    await consumer?.kill();
  });

  it('lands exactly the valid events in the logs table', async () => {
    await waitFor(
      `${VALID_COUNT} rows in logs`,
      async () => (await countLogs()) >= VALID_COUNT || undefined,
    );

    // Malformed messages must not have produced rows.
    expect(await countLogs()).toBe(VALID_COUNT);
  });

  it('preserves the transformed field values', async () => {
    const result = await client.query({
      query: `SELECT service, level, status_code, latency_ms, trace_id, message
              FROM logs ORDER BY trace_id LIMIT 1`,
      format: 'JSONEachRow',
    });
    const [ row ] = await result.json<Record<string, unknown>>();

    expect(row).toMatchObject({
      service: ERROR_SERVICE,
      level: 'ERROR',
      status_code: 503,
      latency_ms: 900,
      trace_id: 'trace-0',
      message: 'event-0',
    });
  });

  it('populates logs_1m through the materialized view', async () => {
    const totals = await waitFor('the materialized view to aggregate', async () => {
      const result = await client.query({
        query: `SELECT service, sum(total_count) AS total, sum(error_count) AS errors
                FROM logs_1m GROUP BY service ORDER BY service`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ service: string; total: string; errors: string }>();
      const sum = rows.reduce((acc, r) => acc + Number(r.total), 0);
      return sum === VALID_COUNT ? rows : undefined;
    });

    const byService = Object.fromEntries(totals.map((r) => [ r.service, r ]));

    expect(Number(byService[ERROR_SERVICE]!.total)).toBe(4);
    expect(Number(byService[ERROR_SERVICE]!.errors)).toBe(4);
    expect(Number(byService[OK_SERVICE]!.total)).toBe(6);
    // 200s must not be counted as errors.
    expect(Number(byService[OK_SERVICE]!.errors)).toBe(0);
  });

  it('merges quantile states into usable percentiles', async () => {
    const result = await client.query({
      query: `SELECT service, quantilesMerge(0.50, 0.90, 0.99)(latency_quantiles) AS p
              FROM logs_1m WHERE service = {service:String} GROUP BY service`,
      query_params: { service: ERROR_SERVICE },
      format: 'JSONEachRow',
    });
    const [ row ] = await result.json<{ p: number[] }>();

    // Every error event carried latency 900.
    expect(row!.p.map(Number)).toEqual([ 900, 900, 900 ]);
  });

  it('routes both malformed messages to the DLQ with their reasons', async () => {
    const raw = await drainTopic(DLQ_TOPIC, 2);
    const envelopes = raw.map((r) => JSON.parse(r));

    expect(envelopes).toHaveLength(2);

    const reasons = envelopes.map((e) => e.reason).sort();
    expect(reasons[0]).toBe('INVALID_JSON_STRING_FORMAT');
    expect(reasons[1]).toMatch(/^SCHEMA_VIOLATION: /);

    const jsonFailure = envelopes.find((e) => e.reason === 'INVALID_JSON_STRING_FORMAT');
    expect(jsonFailure.payload).toBe('MALFORMED_RAW_STREAM_GARBAGE_CRASH_TEST_12345');
    expect(new Date(jsonFailure.rejectedAt).toString()).not.toBe('Invalid Date');
  });

  it('commits every offset once the rows are in, leaving no lag behind', async () => {
    // Includes the trailing malformed messages: if their offsets were left
    // uncommitted the group would sit permanently behind the partition head.
    await waitFor('consumer group lag to reach zero', async () => {
      const offsets = await admin.fetchOffsets({ groupId: GROUP_ID, topics: [ TOPIC ] });
      const topicOffsets = await admin.fetchTopicOffsets(TOPIC);
      const committed = offsets[0]?.partitions ?? [];

      return topicOffsets.every((t) => {
        const c = committed.find((p) => p.partition === t.partition);
        return c && BigInt(c.offset) === BigInt(t.offset);
      }) || undefined;
    });
  });

  it('counts each message exactly once, with no redelivery', async () => {
    // Trailing malformed messages used to be redelivered every few seconds
    // forever, so settle well past that window before asserting the counts held.
    await new Promise((resolve) => setTimeout(resolve, 8_000));

    expect(await consumer.readMetric('pipeline_messages_consumed_total')).toBe(VALID_COUNT + 2);
    expect(await consumer.readMetric('pipeline_messages_dlq_total')).toBe(2);
    expect(await countLogs()).toBe(VALID_COUNT);
  });

  it('stays alive after processing malformed input', () => {
    // A bad payload must never take the process down.
    expect(consumer.child.exitCode).toBeNull();
  });

  it('shuts down cleanly on SIGTERM', async () => {
    const exitCode = await consumer.stop('SIGTERM');

    expect(exitCode).toBe(0);
    expect(consumer.logs()).toContain('Consumer shut down cleanly');
  });
});

describe('graceful shutdown drains the buffer', () => {
  const DRAIN_TOPIC = 'drain-log-events';
  const DRAIN_GROUP = 'drain-consumer';
  const DRAIN_SERVICE = 'drain-service';
  const DRAIN_COUNT = 5;

  let drainConsumer: ConsumerProcess;

  afterAll(async () => {
    await drainConsumer?.kill();
  });

  it('flushes rows that were still buffered when SIGTERM arrived', async () => {
    // Thresholds high enough that neither the size nor the timer trigger fires,
    // so the only thing that can get these rows into ClickHouse is the shutdown
    // drain in the service's stop().
    drainConsumer = await startConsumerProcess({
      brokers,
      clickhouse: connection,
      topic: DRAIN_TOPIC,
      dlqTopic: `${DRAIN_TOPIC}-dlq`,
      groupId: DRAIN_GROUP,
      batchSize: 100_000,
      flushIntervalMs: 600_000,
    });

    await waitFor('the drain consumer group to become stable', async () => {
      const { groups } = await admin.describeGroups([ DRAIN_GROUP ]);
      const group = groups.find((g) => g.groupId === DRAIN_GROUP);
      return (group?.state === 'Stable' && group.members.length > 0) || undefined;
    }, { timeoutMs: 60_000 });

    await producer.send({
      topic: DRAIN_TOPIC,
      messages: Array.from({ length: DRAIN_COUNT }, (_, i) => ({
        value: JSON.stringify({ ...validEvent(i), service: DRAIN_SERVICE }),
      })),
    });

    // Wait until the rows are sitting in the in-memory buffer, unflushed.
    await drainConsumer.waitForMetric(
      'pipeline_buffer_current_size_records',
      (value) => value === DRAIN_COUNT,
      'rows to accumulate in the consumer buffer',
    );

    const countDrained = async () => {
      const result = await client.query({
        query: 'SELECT count() AS c FROM logs WHERE service = {service:String}',
        query_params: { service: DRAIN_SERVICE },
        format: 'JSONEachRow',
      });
      const [ first ] = await result.json<{ c: string }>();
      return Number(first?.c ?? 0);
    };

    // Nothing has reached ClickHouse yet.
    expect(await countDrained()).toBe(0);

    const exitCode = await drainConsumer.stop('SIGTERM');
    expect(exitCode).toBe(0);

    expect(await countDrained()).toBe(DRAIN_COUNT);
  });
});
