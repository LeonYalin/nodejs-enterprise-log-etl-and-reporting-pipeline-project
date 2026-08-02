import { pathToFileURL } from 'node:url';
import { kafkaClient, ensureTopics } from '../lib/kafka.js';
import { clickhouseClient } from '../lib/clickhouse.js';
import { createLogsRepository } from '../lib/logs-repository.js';
import { logger } from '../lib/logger.js';
import { startMetricsServer } from '../lib/metrics-server.js';
import { config } from '../config/index.js';
import { createDlqPublisher } from './dlq.js';
import { createConsumerService } from './service.js';

/**
 * Composition root for the consumer process: builds the real Kafka/ClickHouse
 * dependencies, wires signal handling, and owns the process exit codes.
 */
async function main(): Promise<void> {
  await ensureTopics([ config.KAFKA_TOPIC, config.KAFKA_DLQ_TOPIC ]);

  const service = createConsumerService({
    consumer: kafkaClient.consumer({ groupId: config.KAFKA_GROUP_ID }),
    admin: kafkaClient.admin(),
    dlq: createDlqPublisher({ producer: kafkaClient.producer(), topic: config.KAFKA_DLQ_TOPIC }),
    logs: createLogsRepository(clickhouseClient),
    topic: config.KAFKA_TOPIC,
    groupId: config.KAFKA_GROUP_ID,
    batchSize: config.BATCH_SIZE,
    flushIntervalMs: config.FLUSH_INTERVAL_MS,
  });

  const metricsServer = startMetricsServer(config.CONSUMER_METRICS_PORT);

  let isShuttingDown = false;
  const shutdown = async (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    logger.info({ signal }, 'Shutdown signal received. Draining buffer and committing offsets...');

    try {
      await service.stop();
      await new Promise<void>((resolve) => metricsServer.close(() => resolve()));
      logger.info('Consumer shut down cleanly.');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'Error during consumer shutdown');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  await service.start();
}

// Only self-start when executed directly, so importing this module (e.g. from a
// test) has no side effects.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    logger.fatal({ err }, 'Fatal error during consumer startup');
    process.exit(1);
  });
}
