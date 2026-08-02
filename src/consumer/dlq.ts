import { logger } from '../lib/logger.js';
import { pipelineMetrics } from '../lib/metrics.js';

export interface DlqPublisherDeps {
  /**
   * Typed structurally to just the two methods used, so a real KafkaJS Producer
   * and a plain test object both satisfy it.
   */
  producer: {
    connect(): Promise<void>;
    send(record: { topic: string; messages: { value: string }[] }): Promise<unknown>;
  };
  topic: string;
}

/**
 * Publishes rejected messages to the dead-letter topic.
 *
 * Connects lazily on first use, and never throws: a DLQ failure must not take
 * down the consumer, since the whole point of the DLQ is to absorb bad data.
 */
export function createDlqPublisher({ producer, topic }: DlqPublisherDeps) {
  let isConnected = false;

  return {
    async send(rawMessage: string | null, reason: string): Promise<void> {
      try {
        if (!isConnected) {
          await producer.connect();
          isConnected = true;
        }

        pipelineMetrics.messagesDlq.inc({ reason });
        logger.warn({ reason }, 'Routing invalid event payload to Dead-Letter Queue');

        await producer.send({
          topic,
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
    },
  };
}

export type DlqPublisher = ReturnType<typeof createDlqPublisher>;
