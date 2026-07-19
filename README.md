## Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse)

> Full build order & design → [IMPLEMENTATION.md](IMPLEMENTATION.md).

### The goal

Learn how to handle massive data velocity — ~10,000 log events/sec — on ordinary local
dev hardware, without crashing the app or losing data. That means treating memory,
network sockets, and database connections as scarce resources. Here are the specific
architectural instruments and engineering methods this project uses to handle that load.

**1. High-throughput buffering & batching (the consumer engine)**
Making 10,000 individual database inserts every second would destroy database
performance due to network round-trip overhead.
*   **The Instrument:** an in-memory batch buffer (`src/consumer/batch-buffer.ts`).
*   **The Method:** instead of writing logs to ClickHouse the moment they arrive, the
    consumer accumulates them in RAM and triggers a bulk write only when 5,000 records
    are collected (`BATCH_SIZE`) **or** 1,000ms have passed (`FLUSH_INTERVAL_MS`),
    whichever comes first. At a sustained 10,000/sec, the buffer fills to 5,000 in
    about half a second — so this turns ~10,000 individual writes into roughly 2
    efficient bulk requests per second.

**2. Stream backpressure (resource protection)**
If ClickHouse slows down or spikes in latency, logs back up in the consumer's memory
buffer. If the consumer keeps pulling data from Kafka blindly, it will run out of
memory and crash with an OOM error.
*   **The Instrument:** KafkaJS `eachBatch` lifecycle controls + `pause()`/`resume()`.
*   **The Method:** if the internal memory buffer crosses a safety threshold, the
    consumer actively pauses pulling data from Kafka — the backlog sits safely on disk
    in the Kafka topic. Once a bulk insert to ClickHouse succeeds and clears the
    buffer, the consumer resumes pulling. The pipeline self-throttles under load
    instead of falling over.

**3. Columnar aggregation at scale (the database engine)**
Standard relational databases (Postgres, MySQL) struggle with massive log volumes —
they store data in rows and build heavy indices that slow down as tables grow.
*   **The Instrument:** ClickHouse's `MergeTree` and `AggregatingMergeTree` table engines.
*   **The Method:** ClickHouse stores data in columns, compressing repetitive fields
    (service name, log level) heavily. To serve fast dashboards under load, a
    **Materialized View** intercepts logs as they're inserted and incrementally
    pre-aggregates them into a background summary table (`logs_1m`), instead of
    scanning millions of raw rows on every dashboard refresh. The API queries a few
    dozen pre-aggregated rows instead of the full raw log table.

**4. Reducing ClickHouse insert latency**
Even with batching, waiting for ClickHouse to fully merge each batch to disk before
acknowledging it would slow down how fast the consumer can move on to its next batch —
capping overall throughput. (Note: this isn't about the Node.js event loop being
*blocked* — an `await`'d HTTP call never blocks it — it's about how long each round
trip takes before the consumer can proceed.)
*   **The Instrument:** the `@clickhouse/client` driver with `async_insert` settings.
*   **The Method:** configuring `async_insert: 1` and `wait_for_async_insert: 0` tells
    ClickHouse to accept the batch into its own in-memory queue, acknowledge
    immediately, and flush to disk asynchronously in the background. This keeps each
    insert call fast so the batch-buffer loop can sustain throughput instead of
    stalling on ClickHouse's disk I/O every cycle.

**5. Graceful shutdown & zero data loss**
When a pipeline is processing 10,000 rows/sec, stopping the process abruptly would trap
thousands of records in memory — causing data loss or duplicate reprocessing.
*   **The Instrument:** Node.js `process.on('SIGTERM')` hooks.
*   **The Method:** on shutdown, the consumer stops accepting new messages, force-flushes
    whatever's currently in the memory buffer to ClickHouse, commits the corresponding
    Kafka offsets only *after* that flush succeeds, then disconnects cleanly.

**6. Real-time observability (the metrics stack)**
You can't tune a high-throughput system you're blind to.
*   **The Instrument:** `prom-client` + Prometheus + Grafana.
*   **The Method:** each process (producer, consumer, api) exposes a `/metrics`
    endpoint with deep telemetry. In Grafana you'll be able to see **consumer lag**
    (are we falling behind the producer?), **insert latency** (how long ClickHouse is
    taking to store batches), and **buffer size** (is backpressure actively
    triggering?).

### Tech stack

*   **Broker/Database:** Apache Kafka (KRaft mode), ClickHouse (columnar analytical database).
*   **Node libraries:** `kafkajs`, `@clickhouse/client`, `zod`, `express`, `pino`, `prom-client`, `@faker-js/faker`.
*   **Observability:** Prometheus, Grafana, Kafka UI, ClickHouse Play.

### Status

Greenfield — see [IMPLEMENTATION.md](IMPLEMENTATION.md) for the full build plan and
step-by-step progress. Quickstart commands will land here once the infra
(`docker-compose.yml`) and dev scripts exist.
