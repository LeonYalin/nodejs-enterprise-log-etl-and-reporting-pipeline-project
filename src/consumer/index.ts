import type { EachBatchPayload } from 'kafkajs';
import { kafkaClient, ensureTopics } from '../lib/kafka.js';
import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';
import { startMetricsServer } from '../lib/metrics-server.js';
import { config } from '../config/index.js';
import { RawLogEventSchema, toRow, type ClickHouseLogRow } from '../domain/log-event.js';
import { sendToDLQ } from './dlq.js';
import { addToBuffer, forceFlush, getBufferSize, setOffsetResolver } from './batch-buffer.js';

// Pause a partition once the buffer holds more than this many rows; resume once it drains.
const HIGH_WATERMARK = config.BATCH_SIZE * 3;
// How often (wall-clock) to ping the group coordinator during a large batch --
// well under KafkaJS's default sessionTimeout (30s) with margin to spare.
const HEARTBEAT_INTERVAL_MS = 3000;

const consumer = kafkaClient.consumer({ groupId: config.KAFKA_GROUP_ID });
const admin = kafkaClient.admin();
let isRunning = true;

// resolveOffset/commitOffsetsIfNecessary from the most recent eachBatch call per partition,
// used by the buffer's flush callback to commit only after ClickHouse has the data.
const resolveOffsetByPartition = new Map<number, (offset: string) => void>();
let commitOffsetsIfNecessary: EachBatchPayload['commitOffsetsIfNecessary'] | null = null;
let uncommittedOffsets: EachBatchPayload['uncommittedOffsets'] | null = null;
const pausedPartitions = new Set<number>();

setOffsetResolver(async (maxOffsetByPartition) => {
  for (const [ partition, offset ] of maxOffsetByPartition) {
    resolveOffsetByPartition.get(partition)?.(offset);
  }
  // commitOffsetsIfNecessary() called with NO args is gated by autoCommitInterval/
  // autoCommitThreshold, both unset here (we only pass autoCommit: false) -- so it's a
  // silent permanent no-op. Passing explicit offsets routes KafkaJS's runner to
  // consumerGroup.commitOffsets(offsets) instead, an unconditional commit. See
  // node_modules/kafkajs/src/consumer/runner.js (commitOffsetsIfNecessary) and
  // offsetManager/index.js (commitOffsets/commitOffsetsIfNecessary).
  const offsets = uncommittedOffsets?.();
  if (offsets) {
    await commitOffsetsIfNecessary?.(offsets);
  }

  if (getBufferSize() < HIGH_WATERMARK && pausedPartitions.size > 0) {
    for (const partition of pausedPartitions) {
      consumer.resume([ { topic: config.KAFKA_TOPIC, partitions: [ partition ] } ]);
    }
    pausedPartitions.clear();
  }
});

/** Background worker loop to continuously check and record partition lag */
async function trackLag() {
  while (isRunning) {
    try {
      const topicOffsets = await admin.fetchTopicOffsets(config.KAFKA_TOPIC);
      const [ groupOffsets ] = await admin.fetchOffsets({ groupId: config.KAFKA_GROUP_ID, topics: [ config.KAFKA_TOPIC ] });
      const consumerOffsets = groupOffsets?.partitions ?? [];

      for (const topicOffset of topicOffsets) {
        const consumerOffset = consumerOffsets.find((p) => p.partition === topicOffset.partition);
        if (consumerOffset) {
          const lag = BigInt(topicOffset.offset) - BigInt(consumerOffset.offset);
          pipelineMetrics.consumerLag.set({ partition: topicOffset.partition }, Number(lag));
        }
      }
    } catch (err) {
      logger.warn({ err }, 'Lag monitoring pass failed');
    }
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
}

async function startConsumer() {
  await ensureTopics([ config.KAFKA_TOPIC, config.KAFKA_DLQ_TOPIC ]);

  await consumer.connect();
  await admin.connect();
  await consumer.subscribe({ topic: config.KAFKA_TOPIC, fromBeginning: false });

  startMetricsServer(config.CONSUMER_METRICS_PORT);
  void trackLag();

  logger.info('Kafka consumer connected. Listening for events...');

  await consumer.run({
    autoCommit: false,
    // Without this, KafkaJS resolves the batch's *last* offset as soon as eachBatch
    // returns -- which happens immediately here, before the buffered rows are ever
    // flushed to ClickHouse. That silently undermined "commit only after a successful
    // insert" for every valid message sitting in the not-yet-flushed buffer.
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch, resolveOffset, heartbeat, isRunning: checkRunning,
      commitOffsetsIfNecessary: commit, uncommittedOffsets: getUncommittedOffsets,
    }) => {
      resolveOffsetByPartition.set(batch.partition, resolveOffset);
      commitOffsetsIfNecessary = commit;
      uncommittedOffsets = getUncommittedOffsets;

      if (getBufferSize() > HIGH_WATERMARK && !pausedPartitions.has(batch.partition)) {
        logger.warn({ partition: batch.partition }, 'Buffer above high watermark; pausing partition');
        consumer.pause([ { topic: batch.topic, partitions: [ batch.partition ] } ]);
        pausedPartitions.add(batch.partition);
      }

      const validRows: ClickHouseLogRow[] = [];
      let maxValidOffset: string | null = null;
      let lastHeartbeatAt = Date.now();

      for (const message of batch.messages) {
        if (!checkRunning() || !isRunning) break;

        // Ping the group coordinator on a wall-clock interval, not per message or per
        // message count: awaiting a network round-trip on every single message capped
        // throughput to ~1-2k/sec (vs. the producer's 10k/sec), which showed up as
        // unbounded consumer lag. A message-count threshold isn't a safe replacement
        // either -- it doesn't bound elapsed time, so a batch with lots of slow,
        // awaited DLQ sends could still exceed the broker's session timeout between
        // heartbeats even with a "periodic" count-based check.
        if (Date.now() - lastHeartbeatAt > HEARTBEAT_INTERVAL_MS) {
          await heartbeat();
          lastHeartbeatAt = Date.now();
        }

        pipelineMetrics.messagesConsumed.inc({ topic: batch.topic });
        const rawString = message.value ? message.value.toString() : null;

        if (!rawString) {
          await sendToDLQ(null, 'EMPTY_KAFKA_PAYLOAD');
          resolveOffset(message.offset);
          continue;
        }

        let parsedJSON: unknown;
        try {
          parsedJSON = JSON.parse(rawString);
        } catch {
          await sendToDLQ(rawString, 'INVALID_JSON_STRING_FORMAT');
          resolveOffset(message.offset);
          continue;
        }

        const validationResult = RawLogEventSchema.safeParse(parsedJSON);
        if (!validationResult.success) {
          await sendToDLQ(rawString, `SCHEMA_VIOLATION: ${validationResult.error.message}`);
          resolveOffset(message.offset);
          continue;
        }

        // Do NOT resolveOffset here: this message's offset is only safe to commit
        // once ClickHouse has durably accepted it, via the flush callback above.
        validRows.push(toRow(validationResult.data));
        maxValidOffset = message.offset;
      }

      if (maxValidOffset) {
        addToBuffer(validRows, batch.partition, maxValidOffset);
      }
    },
  });
}

const shutdown = async (signal: string) => {
  if (!isRunning) return;
  logger.info({ signal }, 'Shutdown signal received. Draining buffer and committing offsets...');
  isRunning = false;

  try {
    await forceFlush();
    if (getBufferSize() > 0) {
      logger.error({ remaining: getBufferSize() }, 'Shutdown flush failed; unflushed rows will be reprocessed on next startup');
    }
    await consumer.disconnect();
    await admin.disconnect();
    logger.info('Consumer shut down cleanly.');
    process.exit(0);
  } catch (err) {
    logger.error({ err }, 'Error during consumer shutdown');
    process.exit(1);
  }
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

startConsumer().catch((err) => {
  logger.fatal({ err }, 'Fatal error during consumer startup');
  process.exit(1);
});
