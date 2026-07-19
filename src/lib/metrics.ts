import client from "prom-client";

// Create a custom registry isolated from the global state
export const registry = new client.Registry();

// Enable default metrics collection (CPU, Memory, Event loop lag)
client.collectDefaultMetrics({ register: registry });

// Pipeline Operational Metrics
export const pipelineMetrics = {
  // Messages metrics
  messagesProduced: new client.Counter({
    name: "pipeline_messages_produced_total",
    help: "Total number of messages produced to Kafka",
    labelNames: [ "topic" ],
    registers: [ registry ],
  }),

  messagesConsumed: new client.Counter({
    name: "pipeline_messages_consumed_total",
    help: "Total number of messages read from Kafka topics",
    labelNames: [ "topic" ],
    registers: [ registry ],
  }),

  messagesDlq: new client.Counter({
    name: "pipeline_messages_dlq_total",
    help: "Total number of invalid messages rerouted to Dead Letter Queue",
    labelNames: [ "reason" ],
    registers: [ registry ],
  }),

  // ClickHouse metrics
  clickhouseBatchInsertDuration: new client.Histogram({
    name: "pipeline_clickhouse_insert_duration_seconds",
    help: "Time taken to execute batch inserts into ClickHouse",
    buckets: [ 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5 ],
    registers: [ registry ],
  }),

  clickhouseBatchSize: new client.Histogram({
    name: "pipeline_clickhouse_batch_size_records",
    help: "Distribution of record counts in successful ClickHouse flushes",
    buckets: [ 100, 500, 1000, 2500, 5000, 10000 ],
    registers: [ registry ],
  }),

  bufferCurrentSize: new client.Gauge({
    name: "pipeline_buffer_current_size_records",
    help: "Current size of in-memory stream validation buffer",
    registers: [ registry ],
  }),

};
