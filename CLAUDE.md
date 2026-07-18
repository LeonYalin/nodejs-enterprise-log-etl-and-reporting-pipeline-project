# Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse)

High-throughput log pipeline: a Kafka **producer** blasts ~10k mock logs/sec → a batch **consumer/ETL** validates, transforms, and bulk-inserts into **ClickHouse** → a ClickHouse **Materialized View** incrementally aggregates → an **Express API** + minimal dashboard serve reports. Prometheus + Grafana + Kafka UI for observability.

> Status: greenfield. Full build order & design → [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Data flow
`producer → Kafka topic → consumer (eachBatch → zod-validate → transform → size/time batch → JSONEachRow insert) → ClickHouse logs → MV → logs_1m → Express /reports/*`
Invalid messages → **DLQ topic** (never crash the consumer).

## Component map
- `src/producer/` — faker generator + rate limiter (KafkaJS idempotent producer).
- `src/consumer/` — `index.ts` (offsets/shutdown), `batch-buffer.ts` (flush on size/time), `transform.ts`, `dlq.ts`.
- `src/api/` — Express server, `routes/reports.ts`, `queries.ts` (reads `logs_1m` via `-Merge`).
- `src/lib/` — shared `logger` (pino), `metrics` (prom-client), `kafka`, `clickhouse` factories.
- `src/config/` — env → zod-validated typed config (the only place that reads `process.env`).
- `clickhouse/init/*.sql` — schema + materialized views.

## Conventions (non-negotiable)
- TypeScript strict, ESM. Env only via `src/config`; logs only via `src/lib/logger`; metrics only via `src/lib/metrics`; ClickHouse writes only via `src/lib/clickhouse` (bulk `JSONEachRow`, never row-by-row).
- **Consumer invariants:** commit offsets *only after* a successful insert (at-least-once); malformed → DLQ, never throw; never block the event loop.
- Env vars load via Node `--env-file=.env` (no `dotenv`).

## How to run / query
- Operational commands (infra up/down, `db:init`, dev procs, load, endpoints) → **`run-pipeline` skill**.
- Diagnostic & report SQL → **`clickhouse-queries` skill**.
- End-to-end verification → **`pipeline-verifier` agent**. ClickHouse design/tuning → **`clickhouse-expert` agent**. Reviewing new TS against the streaming invariants → **`etl-code-reviewer` agent**.

Don't restate commands or SQL here — those skills are the single source.
