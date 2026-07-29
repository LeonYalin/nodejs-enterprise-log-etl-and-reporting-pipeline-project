import { CompressionTypes } from 'kafkajs';
import { faker } from '@faker-js/faker';
import { startMetricsServer } from '../lib/metrics-server.js';
import { kafkaClient, ensureTopics } from '../lib/kafka.js';
import { pipelineMetrics } from '../lib/metrics.js';
import { logger } from '../lib/logger.js';
import { config } from '../config/index.js';
import type { RawLogEvent } from '../domain/log-event.js';

// Configuration
const TICK_INTERVAL_MS = 50; // 20 ticks per second
const BATCH_SIZE_PER_TICK = Math.ceil(config.PRODUCER_RATE / (1000 / TICK_INTERVAL_MS));
const SEND_CHUNK_SIZE = 1000; // cap individual sendBatch calls per plan (500-1000 msgs)

// Shared state flag for shutdown
let isRunning = true;

// Helper to block execution asynchronously
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Generates a mock log event matching RawLogEventSchema, with a realistic
 * status/latency distribution (mostly INFO, some WARN/ERROR, occasional 5xx).
 */
function createMockLog(): RawLogEvent {
  const roll = Math.random();
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
function buildTickMessages(): { value: string }[] {
  const messages: { value: string }[] = [];

  for (let i = 0; i < BATCH_SIZE_PER_TICK; i++) {
    const errorRoll = Math.random();

    if (errorRoll < 0.01) {
      // 1% chance: raw non-JSON garbage
      messages.push({ value: 'MALFORMED_RAW_STREAM_GARBAGE_CRASH_TEST_12345' });
    } else if (errorRoll < 0.02) {
      // 1% chance: syntactically valid JSON that fails schema validation
      messages.push({ value: JSON.stringify({ badField: 'missing required root elements' }) });
    } else {
      // 98% chance: schema-valid payload
      messages.push({ value: JSON.stringify(createMockLog()) });
    }
  }

  return messages;
}

/**
 * Main producer process loop
 */
async function startProducer() {
  await ensureTopics([ config.KAFKA_TOPIC ]);

  const producer = kafkaClient.producer({
    idempotent: true,
    maxInFlightRequests: 1,
  });

  logger.info('Connecting Kafka producer...');
  await producer.connect();
  logger.info('Kafka producer connected successfully.');

  const metricsServer = startMetricsServer(config.PRODUCER_METRICS_PORT);

  logger.info(`Starting ingestion stream generation at target rate: ${config.PRODUCER_RATE}/sec (${BATCH_SIZE_PER_TICK} msgs every ${TICK_INTERVAL_MS}ms)`);

  while (isRunning) {
    const startTime = Date.now();
    const messages = buildTickMessages();

    try {
      // Send in bounded sub-batches so a single tick never ships one giant batch
      for (let i = 0; i < messages.length; i += SEND_CHUNK_SIZE) {
        const chunk = messages.slice(i, i + SEND_CHUNK_SIZE);
        await producer.sendBatch({
          topicMessages: [ { topic: config.KAFKA_TOPIC, messages: chunk } ],
          acks: -1, // all
          compression: CompressionTypes.GZIP,
        });
      }
      pipelineMetrics.messagesProduced.inc({ topic: config.KAFKA_TOPIC }, messages.length);
    } catch (error) {
      pipelineMetrics.producerSendErrors.inc();
      logger.error({ error }, 'Failed to deliver message batch payload to Kafka brokers');
    }

    const elapsed = Date.now() - startTime;
    const sleepTime = TICK_INTERVAL_MS - elapsed;
    if (sleepTime > 0) {
      await sleep(sleepTime);
    } else {
      logger.warn(`Producer execution loop lagging behind schedule by ${Math.abs(sleepTime)}ms.`);
    }
  }

  logger.info('Cleaning up infrastructure connections...');
  await producer.disconnect();
  await new Promise<void>((resolve) => metricsServer.close(() => resolve()));
  logger.info('Producer process gracefully terminated.');
}

const shutdown = async (signal: string) => {
  if (!isRunning) return;
  logger.info(`System signal received: ${signal}. Commencing system drain down...`);
  isRunning = false;
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

startProducer().catch((err) => {
  logger.fatal({ err }, 'Fatal unexpected exception caught during runtime execution loops');
  process.exit(1);
});
