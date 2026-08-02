import type { TestProject } from 'vitest/node';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { ClickHouseContainer, type StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { createClient } from '@clickhouse/client';
import { applyInitScripts } from '../../src/lib/schema-init.js';
import { getFreePort, createTestClickHouseClient, type ClickHouseConnection } from './helpers.js';

const CLICKHOUSE_IMAGE = 'clickhouse/clickhouse-server:24.3';
const KAFKA_IMAGE = 'apache/kafka:3.7.0';
const DATABASE = 'log_pipeline';

declare module 'vitest' {
  interface ProvidedContext {
    kafkaBrokers: string;
    clickhouse: ClickHouseConnection;
  }
}

let kafka: StartedTestContainer;
let clickhouse: StartedClickHouseContainer;

/**
 * Starts a Kafka broker matching the production image.
 *
 * @testcontainers/kafka's KafkaContainer is hardcoded to Confluent images (it
 * shells out to /etc/confluent/docker/run and version-gates on Confluent
 * Platform tags), so apache/kafka needs GenericContainer with the same KAFKA_*
 * block docker-compose.yml already proves out.
 *
 * Advertised listeners must name a host:port reachable from the test process,
 * which isn't knowable after start -- so we reserve a free port up front and
 * bind it on both sides rather than letting Docker choose.
 */
async function startKafka(): Promise<{ container: StartedTestContainer; brokers: string }> {
  const port = await getFreePort();

  const container = await new GenericContainer(KAFKA_IMAGE)
    .withExposedPorts({ container: port, host: port })
    .withEnvironment({
      CLUSTER_ID: 'crSAWIL6R1SOeKxcZdh5Iw',
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'controller,broker',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
      KAFKA_LISTENERS: `PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:${port}`,
      KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://localhost:9092,EXTERNAL://localhost:${port}`,
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
      KAFKA_LOG_DIRS: '/var/lib/kafka/data',
      // A single broker can never satisfy the image's default replication factor
      // of 3, so __consumer_offsets would never be created and every consumer
      // group would hang on "group coordinator not available".
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
    })
    .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/, 1))
    .withStartupTimeout(180_000)
    .start();

  return { container, brokers: `localhost:${port}` };
}

export default async function setup({ provide }: TestProject) {
  const [ kafkaStarted, clickhouseStarted ] = await Promise.all([
    startKafka(),
    new ClickHouseContainer(CLICKHOUSE_IMAGE).withDatabase(DATABASE).start(),
  ]);

  kafka = kafkaStarted.container;
  clickhouse = clickhouseStarted;

  const connection: ClickHouseConnection = {
    url: clickhouse.getHttpUrl(),
    database: DATABASE,
    username: clickhouse.getUsername(),
    password: clickhouse.getPassword(),
  };

  // Apply the real clickhouse/init/*.sql, so the suite tests the shipped schema
  // rather than a copy that can drift.
  const client = createTestClickHouseClient(connection);
  const bootstrapClient = createClient({
    url: connection.url,
    username: connection.username,
    password: connection.password,
  });
  try {
    await applyInitScripts({ client, bootstrapClient, database: DATABASE });
  } finally {
    await client.close();
    await bootstrapClient.close();
  }

  provide('kafkaBrokers', kafkaStarted.brokers);
  provide('clickhouse', connection);

  return async () => {
    await Promise.allSettled([ kafka.stop(), clickhouse.stop() ]);
  };
}
