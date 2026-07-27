import { clickhouseClient } from '../lib/clickhouse.js';
import type { ClickHouseLogRow } from '../domain/log-event.js';
import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';
import { config } from '../config/index.js';

const TABLE = 'logs';

// One chunk per eachBatch call: the rows it produced, plus the highest offset
// among them, so we can resolve/commit that offset only once it's durably in ClickHouse.
interface BufferChunk {
  rows: ClickHouseLogRow[];
  partition: number;
  offset: string;
}

/** Invoked once per successful flush with the highest offset per partition. */
export type OffsetResolver = (maxOffsetByPartition: Map<number, string>) => Promise<void>;

let buffer: BufferChunk[] = [];
let bufferedRowCount = 0;
let flushTimer: NodeJS.Timeout | null = null;
let isFlushing = false;
let onFlush: OffsetResolver | null = null;

export function setOffsetResolver(resolver: OffsetResolver): void {
  onFlush = resolver;
}

export function getBufferSize(): number {
  return bufferedRowCount;
}

export function addToBuffer(rows: ClickHouseLogRow[], partition: number, offset: string): void {
  if (rows.length === 0) return;

  buffer.push({ rows, partition, offset });
  bufferedRowCount += rows.length;
  pipelineMetrics.bufferCurrentSize.set(bufferedRowCount);

  if (bufferedRowCount >= config.BATCH_SIZE) {
    void forceFlush();
  } else if (!flushTimer) {
    flushTimer = setTimeout(() => {
      void forceFlush();
    }, config.FLUSH_INTERVAL_MS);
  }
}

export async function forceFlush(): Promise<void> {
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
    await clickhouseClient.insert({ table: TABLE, values: rows, format: 'JSONEachRow' });

    endTimer();
    pipelineMetrics.clickhouseBatchSize.observe(rows.length);
    logger.info({ count: rows.length }, 'Flushed batch to ClickHouse');

    const maxOffsetByPartition = new Map<number, string>();
    for (const chunk of chunks) {
      const current = maxOffsetByPartition.get(chunk.partition);
      if (!current || BigInt(chunk.offset) > BigInt(current)) {
        maxOffsetByPartition.set(chunk.partition, chunk.offset);
      }
    }

    if (onFlush) {
      await onFlush(maxOffsetByPartition);
    }
  } catch (error) {
    endTimer();
    // Put the chunks back so nothing is lost; they'll be retried on the next flush trigger.
    // Never rethrow: forceFlush() is invoked fire-and-forget from addToBuffer(), and a thrown
    // error here would surface as an unhandled rejection and crash the consumer process.
    buffer = [ ...chunks, ...buffer ];
    bufferedRowCount += rows.length;
    pipelineMetrics.bufferCurrentSize.set(bufferedRowCount);
    logger.error({ error }, 'ClickHouse bulk insert failed; items restored to buffer for retry');
  } finally {
    isFlushing = false;
  }
}
