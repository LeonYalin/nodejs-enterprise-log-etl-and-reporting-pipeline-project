import { CompressionTypes } from 'kafkajs';
import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';
import {
  buildTickMessages,
  messagesPerTick,
  SEND_CHUNK_SIZE,
  TICK_INTERVAL_MS,
  type Random,
} from './generator.js';

export interface ProducerServiceDeps {
  /** Typed structurally to the methods used, so a plain object satisfies it too. */
  producer: {
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    sendBatch(record: {
      topicMessages: { topic: string; messages: { value: string }[] }[];
      acks?: number;
      compression?: CompressionTypes;
    }): Promise<unknown>;
  };
  topic: string;
  /** Target messages per second (config.PRODUCER_RATE). */
  rate: number;
  tickIntervalMs?: number;
  sendChunkSize?: number;
  random?: Random;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Drives the mock ingestion load: every tick it generates a batch and ships it
 * to Kafka in bounded sub-batches, throttling to hold the target rate.
 */
export function createProducerService(deps: ProducerServiceDeps) {
  const {
    producer,
    topic,
    rate,
    tickIntervalMs = TICK_INTERVAL_MS,
    sendChunkSize = SEND_CHUNK_SIZE,
    random,
  } = deps;

  const batchSizePerTick = messagesPerTick(rate, tickIntervalMs);

  let isRunning = false;
  let loopDone: Promise<void> | null = null;

  /**
   * One tick: generate a batch and ship it. Never throws -- a broker hiccup is
   * recorded and the loop carries on rather than killing the process.
   */
  async function runTick(): Promise<void> {
    const messages = buildTickMessages(batchSizePerTick, random);

    try {
      // Send in bounded sub-batches so a single tick never ships one giant batch
      for (let i = 0; i < messages.length; i += sendChunkSize) {
        const chunk = messages.slice(i, i + sendChunkSize);
        await producer.sendBatch({
          topicMessages: [ { topic, messages: chunk } ],
          acks: -1, // all
          compression: CompressionTypes.GZIP,
        });
      }
      pipelineMetrics.messagesProduced.inc({ topic }, messages.length);
    } catch (error) {
      pipelineMetrics.producerSendErrors.inc();
      logger.error({ error }, 'Failed to deliver message batch payload to Kafka brokers');
    }
  }

  async function runLoop(): Promise<void> {
    while (isRunning) {
      const startTime = Date.now();
      await runTick();

      const elapsed = Date.now() - startTime;
      const sleepTime = tickIntervalMs - elapsed;
      if (sleepTime > 0) {
        await sleep(sleepTime);
      } else {
        logger.warn(`Producer execution loop lagging behind schedule by ${Math.abs(sleepTime)}ms.`);
      }
    }
  }

  /** Connects and runs until stop() is called. */
  async function start(): Promise<void> {
    logger.info('Connecting Kafka producer...');
    await producer.connect();
    logger.info('Kafka producer connected successfully.');

    logger.info(
      `Starting ingestion stream generation at target rate: ${rate}/sec ` +
      `(${batchSizePerTick} msgs every ${tickIntervalMs}ms)`,
    );

    isRunning = true;
    loopDone = runLoop();
    await loopDone;
  }

  /**
   * Stops after the in-flight tick completes, then disconnects. Does not exit the
   * process -- that's the entrypoint's call to make.
   */
  async function stop(): Promise<void> {
    if (!isRunning) return;
    isRunning = false;

    await loopDone;
    await producer.disconnect();
  }

  return { start, stop, runTick, batchSizePerTick };
}

export type ProducerService = ReturnType<typeof createProducerService>;
