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
