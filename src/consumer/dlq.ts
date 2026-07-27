import { kafkaClient } from '../lib/kafka.js';
import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';
import { config } from '../config/index.js';

const producer = kafkaClient.producer();
let isConnected = false;

export async function sendToDLQ(rawMessage: string | null, reason: string) {
  try {
    if (!isConnected) {
      await producer.connect();
      isConnected = true;
    }

    pipelineMetrics.messagesDlq.inc({ reason });
    logger.warn({ reason }, 'Routing invalid event payload to Dead-Letter Queue');

    await producer.send({
      topic: config.KAFKA_DLQ_TOPIC,
      messages: [ {
        value: JSON.stringify({
          rejectedAt: new Date().toISOString(),
          reason,
          payload: rawMessage,
        }),
      } ],
    });
  } catch (error) {
    logger.error({ error }, 'Failed to route message to DLQ topic');
  }
}
