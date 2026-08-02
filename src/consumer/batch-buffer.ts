import type { ClickHouseLogRow } from '../domain/log-event.js';
import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';

// One chunk per eachBatch call: the rows it produced, plus the highest offset
// among them, so we can resolve/commit that offset only once it's durably in ClickHouse.
interface BufferChunk {
  rows: ClickHouseLogRow[];
  partition: number;
  offset: string;
}

/** Invoked once per successful flush with the highest offset per partition. */
export type OffsetResolver = (maxOffsetByPartition: Map<number, string>) => Promise<void>;

export interface BatchBufferDeps {
  /**
   * The sink rows are flushed to. Typed structurally (not as the concrete
   * repository) so tests can pass a plain object literal.
   */
  logs: { insertMany(rows: ClickHouseLogRow[]): Promise<void> };
  batchSize: number;
  flushIntervalMs: number;
  onFlush?: OffsetResolver;
}

/**
 * Accumulates rows across eachBatch calls and flushes them to ClickHouse in one
 * bulk insert when either the size threshold or the time interval is hit --
 * whichever comes first.
 *
 * State lives in this closure rather than at module scope so each process (and
 * each test) owns an independent buffer.
 */
export function createBatchBuffer({ logs, batchSize, flushIntervalMs, onFlush }: BatchBufferDeps) {
  let buffer: BufferChunk[] = [];
  let bufferedRowCount = 0;
  let flushTimer: NodeJS.Timeout | null = null;
  let isFlushing = false;

  function size(): number {
    return bufferedRowCount;
  }

  async function forceFlush(): Promise<void> {
    if (isFlushing || buffer.length === 0) return;

    isFlushing = true;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }

    const chunks = buffer;
    buffer = [];
    bufferedRowCount = 0;
    pipelineMetrics.bufferCurrentSize.set(0);

    const rows = chunks.flatMap((chunk) => chunk.rows);
    const endTimer = pipelineMetrics.clickhouseBatchInsertDuration.startTimer();
    try {
      // Chunks can be row-free: a batch of nothing but malformed messages still
      // carries an offset checkpoint that has to travel through the flush path,
      // so its offsets are never resolved ahead of rows still awaiting insert.
      if (rows.length > 0) {
        await logs.insertMany(rows);
        pipelineMetrics.clickhouseBatchSize.observe(rows.length);
        logger.info({ count: rows.length }, 'Flushed batch to ClickHouse');
      }
      endTimer();

      const maxOffsetByPartition = new Map<number, string>();
      for (const chunk of chunks) {
        const current = maxOffsetByPartition.get(chunk.partition);
        if (!current || BigInt(chunk.offset) > BigInt(current)) {
          maxOffsetByPartition.set(chunk.partition, chunk.offset);
        }
      }

      // Only now that ClickHouse has durably accepted the rows is it safe to
      // resolve/commit their offsets (at-least-once delivery).
      if (onFlush) {
        await onFlush(maxOffsetByPartition);
      }
    } catch (error) {
      endTimer();
      // Put the chunks back so nothing is lost; they'll be retried on the next flush trigger.
      // Never rethrow: forceFlush() is invoked fire-and-forget from add(), and a thrown
      // error here would surface as an unhandled rejection and crash the consumer process.
      buffer = [ ...chunks, ...buffer ];
      bufferedRowCount += rows.length;
      pipelineMetrics.bufferCurrentSize.set(bufferedRowCount);
      logger.error({ error }, 'ClickHouse bulk insert failed; items restored to buffer for retry');
    } finally {
      isFlushing = false;
    }
  }

  /**
   * Buffers a batch's rows together with the offset that becomes safe to commit
   * once they are flushed. `rows` may be empty -- the offset checkpoint still
   * has to be recorded so it commits in flush order.
   */
  function add(rows: ClickHouseLogRow[], partition: number, offset: string): void {
    buffer.push({ rows, partition, offset });
    bufferedRowCount += rows.length;
    pipelineMetrics.bufferCurrentSize.set(bufferedRowCount);

    if (bufferedRowCount >= batchSize) {
      void forceFlush();
    } else if (!flushTimer) {
      flushTimer = setTimeout(() => {
        void forceFlush();
      }, flushIntervalMs);
    }
  }

  return { add, forceFlush, size };
}

export type BatchBuffer = ReturnType<typeof createBatchBuffer>;
