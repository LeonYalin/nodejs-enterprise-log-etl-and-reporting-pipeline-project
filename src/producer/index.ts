import { pathToFileURL } from 'node:url';
import { kafkaClient, ensureTopics } from '../lib/kafka.js';
import { startMetricsServer } from '../lib/metrics-server.js';
import { logger } from '../lib/logger.js';
import { config } from '../config/index.js';
import { createProducerService } from './service.js';

/**
 * Composition root for the producer process: builds the real Kafka dependencies,
 * wires signal handling, and owns the process exit codes.
 */
async function main(): Promise<void> {
  await ensureTopics([ config.KAFKA_TOPIC ]);

  const service = createProducerService({
    producer: kafkaClient.producer({ idempotent: true, maxInFlightRequests: 1 }),
    topic: config.KAFKA_TOPIC,
    rate: config.PRODUCER_RATE,
  });

  const metricsServer = startMetricsServer(config.PRODUCER_METRICS_PORT);

  let isShuttingDown = false;
  const shutdown = async (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    logger.info({ signal }, 'System signal received. Commencing system drain down...');

    try {
      await service.stop();
      await new Promise<void>((resolve) => metricsServer.close(() => resolve()));
      logger.info('Producer process gracefully terminated.');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'Error during producer shutdown');
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
    logger.fatal({ err }, 'Fatal unexpected exception caught during runtime execution loops');
    process.exit(1);
  });
}
