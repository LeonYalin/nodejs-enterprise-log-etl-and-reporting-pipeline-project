import { describe, it, expect, beforeEach } from 'vitest';
import { CompressionTypes } from 'kafkajs';
import { createProducerService } from './service.js';
import { pipelineMetrics } from '../lib/metrics.js';
import { RawLogEventSchema } from '../domain/log-event.js';

const TOPIC = 'log-events';

/**
 * Stand-in for a KafkaJS producer. Hand-written so it can reject on demand --
 * the one thing a real broker won't reliably do.
 */
function createFakeProducer() {
  const batches: { topic: string; messages: { value: string }[] }[] = [];
  let connects = 0;
  let disconnects = 0;
  let sendFails = false;
  let lastRecord: { acks?: number; compression?: CompressionTypes } | undefined;

  return {
    batches,
    get connects() { return connects; },
    get disconnects() { return disconnects; },
    get lastRecord() { return lastRecord; },
    failSend() { sendFails = true; },
    healSend() { sendFails = false; },
    /** Every message across every sendBatch call. */
    allMessages() { return batches.flatMap((b) => b.messages); },
    async connect(): Promise<void> { connects++; },
    async disconnect(): Promise<void> { disconnects++; },
    async sendBatch(record: {
      topicMessages: { topic: string; messages: { value: string }[] }[];
      acks?: number;
      compression?: CompressionTypes;
    }): Promise<unknown> {
      if (sendFails) throw new Error('broker unavailable');
      for (const tm of record.topicMessages) {
        batches.push({ topic: tm.topic, messages: tm.messages });
      }
      lastRecord = record;
      return [];
    },
  };
}

async function producedCount(topic: string): Promise<number> {
  const metric = await pipelineMetrics.messagesProduced.get();
  return metric.values.find((v) => v.labels.topic === topic)?.value ?? 0;
}

async function sendErrorCount(): Promise<number> {
  const metric = await pipelineMetrics.producerSendErrors.get();
  return metric.values[0]?.value ?? 0;
}

describe('createProducerService', () => {
  beforeEach(() => {
    pipelineMetrics.messagesProduced.reset();
    pipelineMetrics.producerSendErrors.reset();
  });

  it('derives the per-tick batch size from the target rate', () => {
    const service = createProducerService({
      producer: createFakeProducer(), topic: TOPIC, rate: 10_000, tickIntervalMs: 50,
    });

    expect(service.batchSizePerTick).toBe(500);
  });

  it('sends one tick worth of messages to the configured topic', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 2000, tickIntervalMs: 50,
    });

    await service.runTick();

    expect(producer.allMessages()).toHaveLength(100);
    expect(producer.batches.every((b) => b.topic === TOPIC)).toBe(true);
  });

  it('splits a large tick into bounded sub-batches', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 50_000, tickIntervalMs: 50, sendChunkSize: 1000,
    });

    // 2500 messages per tick -> 1000 + 1000 + 500
    await service.runTick();

    expect(service.batchSizePerTick).toBe(2500);
    expect(producer.batches.map((b) => b.messages.length)).toEqual([ 1000, 1000, 500 ]);
    expect(producer.allMessages()).toHaveLength(2500);
  });

  it('sends a single batch when the tick fits in one chunk', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 200, tickIntervalMs: 50, sendChunkSize: 1000,
    });

    await service.runTick();

    expect(producer.batches).toHaveLength(1);
  });

  it('requests full acks and gzip compression', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 100, tickIntervalMs: 50,
    });

    await service.runTick();

    expect(producer.lastRecord?.acks).toBe(-1);
    expect(producer.lastRecord?.compression).toBe(CompressionTypes.GZIP);
  });

  it('counts the messages it produced', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 1000, tickIntervalMs: 50,
    });

    await service.runTick();

    expect(await producedCount(TOPIC)).toBe(50);
  });

  it('never throws when a send fails, and records the error instead', async () => {
    const producer = createFakeProducer();
    producer.failSend();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 1000, tickIntervalMs: 50,
    });

    // A broker hiccup must not kill the load generator's loop.
    await expect(service.runTick()).resolves.toBeUndefined();

    expect(await sendErrorCount()).toBe(1);
    // Nothing was delivered, so nothing should be counted as produced.
    expect(await producedCount(TOPIC)).toBe(0);
  });

  it('keeps producing after a failed tick', async () => {
    const producer = createFakeProducer();
    producer.failSend();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 1000, tickIntervalMs: 50,
    });

    await service.runTick();
    producer.healSend();
    await service.runTick();

    expect(await producedCount(TOPIC)).toBe(50);
    expect(await sendErrorCount()).toBe(1);
  });

  it('emits payloads that satisfy the wire schema', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 200, tickIntervalMs: 50, random: () => 0.5,
    });

    await service.runTick();

    for (const message of producer.allMessages()) {
      expect(RawLogEventSchema.safeParse(JSON.parse(message.value)).success).toBe(true);
    }
  });

  it('connects, runs, then disconnects on stop', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 200, tickIntervalMs: 1,
    });

    const started = service.start();
    // Let a few ticks run before asking it to wind down.
    await new Promise((resolve) => setTimeout(resolve, 20));
    await service.stop();
    await started;

    expect(producer.connects).toBe(1);
    expect(producer.disconnects).toBe(1);
    expect(producer.batches.length).toBeGreaterThan(0);
  });

  it('stops producing once stopped', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({
      producer, topic: TOPIC, rate: 200, tickIntervalMs: 1,
    });

    const started = service.start();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await service.stop();
    await started;

    const afterStop = producer.batches.length;
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(producer.batches.length).toBe(afterStop);
  });

  it('is a no-op when stopped before it ever started', async () => {
    const producer = createFakeProducer();
    const service = createProducerService({ producer, topic: TOPIC, rate: 200 });

    await expect(service.stop()).resolves.toBeUndefined();
    expect(producer.disconnects).toBe(0);
  });
});
