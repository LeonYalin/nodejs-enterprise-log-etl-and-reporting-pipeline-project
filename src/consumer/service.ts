import type { Admin, Consumer, EachBatchPayload } from 'kafkajs';
import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';
import type { ClickHouseLogRow } from '../domain/log-event.js';
import { classifyMessage } from './transform.js';
import { createBatchBuffer, type BatchBufferDeps } from './batch-buffer.js';
import type { DlqPublisher } from './dlq.js';

// How often (wall-clock) to ping the group coordinator during a large batch --
// well under KafkaJS's default sessionTimeout (30s) with margin to spare.
const HEARTBEAT_INTERVAL_MS = 3000;
const LAG_POLL_INTERVAL_MS = 5000;

export interface ConsumerServiceDeps {
  consumer: Consumer;
  admin: Admin;
  dlq: DlqPublisher;
  logs: BatchBufferDeps['logs'];
  topic: string;
  groupId: string;
  batchSize: number;
  flushIntervalMs: number;
}

/**
 * Wires Kafka consumption to the ClickHouse batch buffer.
 *
 * Holds the pipeline's core delivery invariants: offsets are resolved and
 * committed only after ClickHouse durably accepts the rows, malformed messages
 * go to the DLQ instead of crashing, and the partition is paused when the buffer
 * runs ahead of the sink.
 */
export function createConsumerService(deps: ConsumerServiceDeps) {
  const { consumer, admin, dlq, logs, topic, groupId, batchSize, flushIntervalMs } = deps;

  // Pause a partition once the buffer holds more than this many rows; resume once it drains.
  const highWatermark = batchSize * 3;

  let isRunning = true;

  const pausedPartitions = new Set<number>();

  const buffer = createBatchBuffer({
    logs,
    batchSize,
    flushIntervalMs,
    onFlush: async (maxOffsetByPartition) => {
      // The durability guarantee lives here, not in resolveOffset(): rows are in
      // ClickHouse, so it is now safe to persist their offsets. Commit exactly
      // what this flush covered rather than KafkaJS's uncommittedOffsets(), which
      // would also include later batches still sitting unflushed in the buffer.
      //
      // Committed offsets are "next message to read", hence +1. Note that
      // commitOffsetsIfNecessary() with no arguments would be a silent no-op:
      // it is gated on autoCommitInterval/autoCommitThreshold, neither of which
      // is set (see node_modules/kafkajs/src/consumer/offsetManager/index.js).
      const offsets = [ ...maxOffsetByPartition ].map(([ partition, offset ]) => ({
        topic,
        partition,
        offset: (BigInt(offset) + 1n).toString(),
      }));

      if (offsets.length > 0) {
        await consumer.commitOffsets(offsets);
      }

      if (buffer.size() < highWatermark && pausedPartitions.size > 0) {
        for (const partition of pausedPartitions) {
          consumer.resume([ { topic, partitions: [ partition ] } ]);
        }
        pausedPartitions.clear();
      }
    },
  });

  /** Background worker loop to continuously check and record partition lag */
  async function trackLag(): Promise<void> {
    while (isRunning) {
      try {
        const topicOffsets = await admin.fetchTopicOffsets(topic);
        const [ groupOffsets ] = await admin.fetchOffsets({ groupId, topics: [ topic ] });
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
      await new Promise((resolve) => setTimeout(resolve, LAG_POLL_INTERVAL_MS));
    }
  }

  async function handleBatch(payload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, isRunning: checkRunning } = payload;

    if (buffer.size() > highWatermark && !pausedPartitions.has(batch.partition)) {
      logger.warn({ partition: batch.partition }, 'Buffer above high watermark; pausing partition');
      consumer.pause([ { topic: batch.topic, partitions: [ batch.partition ] } ]);
      pausedPartitions.add(batch.partition);
    }

    const validRows: ClickHouseLogRow[] = [];
    let lastProcessedOffset: string | null = null;
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

      const classified = classifyMessage(message.value);
      if (classified.kind === 'invalid') {
        await dlq.send(classified.raw, classified.reason);
      } else {
        validRows.push(classified.row);
      }

      // Resolving is in-memory bookkeeping ("don't hand me this message again"),
      // NOT a durability decision -- that is the commit in onFlush. It has to
      // happen inline: with eachBatchAutoResolve disabled, any offset still
      // unresolved when eachBatch returns is re-fetched, so deferring this to the
      // flush callback makes KafkaJS redeliver the whole batch. Nothing is lost
      // by resolving early, because a crash before the flush leaves the offsets
      // uncommitted and the rows are simply replayed from the last commit.
      resolveOffset(message.offset);
      lastProcessedOffset = message.offset;
    }

    // Recorded even when no rows were valid, so an all-malformed batch still
    // checkpoints its offsets instead of being redelivered indefinitely.
    if (lastProcessedOffset) {
      buffer.add(validRows, batch.partition, lastProcessedOffset);
    }
  }

  async function start(): Promise<void> {
    await consumer.connect();
    await admin.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    void trackLag();

    logger.info('Kafka consumer connected. Listening for events...');

    await consumer.run({
      autoCommit: false,
      // Without this, KafkaJS resolves the batch's *last* offset as soon as eachBatch
      // returns -- which happens immediately here, before the buffered rows are ever
      // flushed to ClickHouse. That silently undermined "commit only after a successful
      // insert" for every valid message sitting in the not-yet-flushed buffer.
      eachBatchAutoResolve: false,
      eachBatch: handleBatch,
    });
  }

  /**
   * Drains the buffer and disconnects. Does not exit the process -- that's the
   * entrypoint's call to make.
   */
  async function stop(): Promise<void> {
    if (!isRunning) return;
    isRunning = false;

    await buffer.forceFlush();
    if (buffer.size() > 0) {
      logger.error(
        { remaining: buffer.size() },
        'Shutdown flush failed; unflushed rows will be reprocessed on next startup',
      );
    }
    await consumer.disconnect();
    await admin.disconnect();
  }

  return { start, stop, handleBatch, bufferSize: buffer.size };
}

export type ConsumerService = ReturnType<typeof createConsumerService>;
