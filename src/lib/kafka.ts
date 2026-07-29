import { Kafka, KafkaConfig } from "kafkajs";
import { config } from "../config/index.js";
import { logger } from "./logger.js";

const kafkaConfig: KafkaConfig = {
  clientId: "enterprise-log-pipeline",
  brokers: config.KAFKA_BROKERS,
  logCreator: () => ({ label, log }) => {
    const { message, ...extra } = log;
    if (label === "ERROR" || label === "NOTHING") {
      logger.error({ ...extra }, message);
    } else if (label === "WARN") {
      logger.warn({ ...extra }, message);
    } else {
      logger.debug({ ...extra }, message);
    }
  }
};

export const kafkaClient = new Kafka(kafkaConfig);

/**
 * Creates the given topics if they don't already exist. Needed because
 * KAFKA_AUTO_CREATE_TOPICS_ENABLE=false in docker-compose.yml, so producer/consumer
 * must create their topics explicitly on boot (kafkajs no-ops on already-existing topics).
 */
export async function ensureTopics(topics: string[]): Promise<void> {
  const admin = kafkaClient.admin();
  await admin.connect();
  try {
    await admin.createTopics({ topics: topics.map((topic) => ({ topic })), waitForLeaders: true });
  } finally {
    await admin.disconnect();
  }
}
