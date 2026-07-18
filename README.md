## Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse)
*   **The Goal:** Master high-throughput data ingestion, streaming transformations, and analytical write-optimization without blocking the event loop.
*   **Production Challenge:** Handling millions of real-time server records and performing instant aggregations without overwhelming relational databases or filling up Node memory.
*   **Tech Stack & Libraries:**
    *   *Broker/Database:* Apache Kafka, ClickHouse (Columnar analytical database).
    *   *Node Libraries:* `kafkajs` (industry-standard Kafka client), `@clickhouse/client` (official ClickHouse driver).
*   **Local Setup & Simulation Plan:**
    *   Spin up Kafka and ClickHouse containers via Docker.
    *   Write a background script acting as a high-frequency Kafka Producer that blasts 10,000 mock log rows/sec into a topic.
    *   Your main Node application consumes messages in batches, transforms the shapes using streams, and executes optimized bulk column-inserts into ClickHouse.
