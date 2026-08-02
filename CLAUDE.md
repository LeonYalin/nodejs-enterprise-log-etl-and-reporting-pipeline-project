# Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse)

High-throughput log pipeline: a Kafka **producer** blasts ~10k mock logs/sec → a batch **consumer/ETL** validates, transforms, and bulk-inserts into **ClickHouse** → a ClickHouse **Materialized View** incrementally aggregates → an **Express API** + minimal dashboard serve reports. Prometheus + Grafana + Kafka UI for observability.

> Status: greenfield. Full build order & design → [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Data flow
`producer → Kafka topic → consumer (eachBatch → zod-validate → transform → size/time batch → JSONEachRow insert) → ClickHouse logs → MV → logs_1m → Express /reports/*`
Invalid messages → **DLQ topic** (never crash the consumer).

## Component map
- `src/producer/` — `index.ts` (composition root + signals), `service.ts` (rate-limited send loop), `generator.ts` (pure mock-log + malformed mix).
- `src/consumer/` — `index.ts` (composition root + signals), `service.ts` (eachBatch/offsets/backpressure), `batch-buffer.ts` (flush on size/time), `transform.ts` (pure classify), `dlq.ts`.
- `src/api/` — `app.ts` (`createApp`, no listen), `index.ts` (entrypoint), `routes/reports.ts`, `queries.ts` (reads `logs_1m` via `-Merge`).
- `src/lib/` — shared `logger` (pino), `metrics` (prom-client), `kafka`, `clickhouse`, `logs-repository` (the only ClickHouse write), `schema-init` (applies `clickhouse/init/*.sql`).
- `src/config/` — env → zod-validated typed config (the only place that reads `process.env`).
- `clickhouse/init/*.sql` — schema + materialized views.
- `tests/integration/` — testcontainers suite; unit tests sit beside their source as `*.test.ts`.

## Conventions (non-negotiable)
- TypeScript strict, ESM. Env only via `src/config`; logs only via `src/lib/logger`; metrics only via `src/lib/metrics`; ClickHouse writes only via `src/lib/logs-repository` (bulk `JSONEachRow`, never row-by-row).
- **Consumer invariants:** `commitOffsets` *only after* a successful insert (at-least-once) — `resolveOffset` is in-memory progress and must stay inline, or KafkaJS redelivers the batch; malformed → DLQ, never throw; never block the event loop.
- **Dependency injection:** modules export `createX(deps)` factories; entrypoints are the only place that builds real clients and the only place with import-time side effects (guarded by `import.meta.url === pathToFileURL(process.argv[1]).href`). Config is the one deliberate singleton.
- **Tests:** no module mocking (`vi.mock` must stay at zero) — inject a fake, or use a real container. Fakes only for what a real dependency can't do (failure injection, timer control).
- Env vars load via Node `--env-file=.env` (no `dotenv`).

## How to run / query
- Operational commands (infra up/down, `db:init`, dev procs, load, endpoints) → **`run-pipeline` skill**.
- Diagnostic & report SQL → **`clickhouse-queries` skill**.
- End-to-end verification → **`pipeline-verifier` agent**. ClickHouse design/tuning → **`clickhouse-expert` agent**. Reviewing new TS against the streaming invariants → **`etl-code-reviewer` agent**.
- Interactive Kafka/Grafana/ClickHouse access → MCP servers in `.mcp.json` (Docker-based, project-scoped). Tool schemas load on demand via Claude Code's tool search, so they add negligible context per turn.

Don't restate commands or SQL here — those skills are the single source.
