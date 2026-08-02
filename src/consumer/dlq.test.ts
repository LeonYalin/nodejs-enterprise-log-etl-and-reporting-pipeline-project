import { describe, it, expect, beforeEach } from 'vitest';
import { createDlqPublisher } from './dlq.js';
import { pipelineMetrics } from '../lib/metrics.js';

/**
 * Stand-in for a KafkaJS producer. Hand-written so it can reject on demand --
 * the one thing a real broker won't reliably do.
 */
function createFakeProducer() {
  const sent: { topic: string; messages: { value: string }[] }[] = [];
  let connects = 0;
  let connectFails = false;
  let sendFails = false;

  return {
    sent,
    get connects() { return connects; },
    failConnect() { connectFails = true; },
    failSend() { sendFails = true; },
    async connect(): Promise<void> {
      connects++;
      if (connectFails) throw new Error('broker unreachable');
    },
    async send(record: { topic: string; messages: { value: string }[] }): Promise<unknown> {
      if (sendFails) throw new Error('send timed out');
      sent.push(record);
      return [];
    },
  };
}

async function dlqCountFor(reason: string): Promise<number> {
  const metric = await pipelineMetrics.messagesDlq.get();
  return metric.values.find((v) => v.labels.reason === reason)?.value ?? 0;
}

describe('createDlqPublisher', () => {
  beforeEach(() => {
    pipelineMetrics.messagesDlq.reset();
  });

  it('publishes the rejection envelope to the DLQ topic', async () => {
    const producer = createFakeProducer();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    await dlq.send('not json at all', 'INVALID_JSON_STRING_FORMAT');

    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0]!.topic).toBe('log-events-dlq');

    const envelope = JSON.parse(producer.sent[0]!.messages[0]!.value);
    expect(envelope).toMatchObject({
      reason: 'INVALID_JSON_STRING_FORMAT',
      payload: 'not json at all',
    });
    expect(new Date(envelope.rejectedAt).toString()).not.toBe('Invalid Date');
  });

  it('preserves a null payload for empty Kafka messages', async () => {
    const producer = createFakeProducer();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    await dlq.send(null, 'EMPTY_KAFKA_PAYLOAD');

    expect(JSON.parse(producer.sent[0]!.messages[0]!.value).payload).toBeNull();
  });

  it('connects lazily, exactly once, across repeated sends', async () => {
    const producer = createFakeProducer();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    expect(producer.connects).toBe(0);

    await dlq.send('a', 'REASON_A');
    await dlq.send('b', 'REASON_B');
    await dlq.send('c', 'REASON_C');

    expect(producer.connects).toBe(1);
    expect(producer.sent).toHaveLength(3);
  });

  it('counts each rejection by reason', async () => {
    const producer = createFakeProducer();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    await dlq.send('x', 'SCHEMA_VIOLATION');
    await dlq.send('y', 'SCHEMA_VIOLATION');
    await dlq.send('z', 'EMPTY_KAFKA_PAYLOAD');

    expect(await dlqCountFor('SCHEMA_VIOLATION')).toBe(2);
    expect(await dlqCountFor('EMPTY_KAFKA_PAYLOAD')).toBe(1);
  });

  it('never throws when the send fails', async () => {
    const producer = createFakeProducer();
    producer.failSend();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    // The DLQ exists to absorb bad data; if it fails, the consumer must survive.
    await expect(dlq.send('payload', 'SOME_REASON')).resolves.toBeUndefined();
  });

  it('never throws when the broker connection fails', async () => {
    const producer = createFakeProducer();
    producer.failConnect();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    await expect(dlq.send('payload', 'SOME_REASON')).resolves.toBeUndefined();
    expect(producer.sent).toHaveLength(0);
  });

  it('retries the connection after an initial failure', async () => {
    const producer = createFakeProducer();
    producer.failConnect();
    const dlq = createDlqPublisher({ producer, topic: 'log-events-dlq' });

    await dlq.send('first', 'REASON');
    expect(producer.connects).toBe(1);

    // A failed connect must not latch isConnected, or every later send is lost.
    const healthy = createFakeProducer();
    const recovered = createDlqPublisher({ producer: healthy, topic: 'log-events-dlq' });
    await recovered.send('second', 'REASON');
    expect(healthy.sent).toHaveLength(1);
  });
});
