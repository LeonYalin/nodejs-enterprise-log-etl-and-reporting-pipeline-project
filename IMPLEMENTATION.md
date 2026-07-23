# Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse) — Implementation Plan

> This document is the executable build spec. The Claude Code config files
> (`CLAUDE.md`, `.claude/`) described in Step 10 already exist in the repo;
> everything else here is still to be implemented.

## Context

The repo starts essentially empty (only `README.md` + `.gitignore`). The README sketches a high-throughput log ingestion story — a Kafka producer blasting ~10k mock log rows/sec, a Node consumer that batch-transforms and bulk-inserts into ClickHouse — but under-specifies the **reporting** side and the production-grade concerns (backpressure, validation, shutdown, observability).

This plan builds that project from scratch as a **small but production-shaped learning app**: a real streaming ingest path, ClickHouse-idiomatic incremental aggregation via Materialized Views, an Express reporting API with a minimal dashboard, and a full local observability stack. Everything runs locally via `docker-compose` (infra) + npm scripts (Node processes). Goal: learn Kafka + ClickHouse + Node streaming best practices without over-engineering.

### Decisions locked in
- **Language:** TypeScript (strict), `tsx` for dev, `zod` for runtime validation.
- **Reporting:** ClickHouse Materialized Views (incremental aggregation) → Express REST API → minimal static HTML dashboard.
- **Claude config:** `CLAUDE.md` + `.claude/` (settings, skills, subagents), authored DRY/token-lean — context lives once, everything else points to it.
- **Observability:** pino logs, `prom-client` metrics, Prometheus + Grafana, plus Kafka UI console + ClickHouse Play for inspection.
- **Reliability:** zod schema validation + Dead-Letter Queue topic; backpressure (pause/resume, size+time batch flush) + graceful shutdown (drain + commit on SIGTERM).
- **Testing:** Vitest unit tests (transform, validation, batch-buffer) + a testcontainers integration test for the ingest→query path.
- **Env loading:** no `dotenv` dependency — Node's built-in `--env-file=.env` in npm scripts, then zod-validated in `src/config`.
- **Out of scope (for now):** CI workflow, ESLint. (Prettier/tsconfig included for basic hygiene.)

---

## Architecture

```
┌──────────────┐   log-events    ┌──────────────────────────┐   bulk insert   ┌───────────────┐
│  Producer    │ ───────────────▶│  Consumer / ETL          │ ───────────────▶│  ClickHouse   │
│ (faker gen,  │   Kafka topic   │  eachBatch → validate →  │  JSONEachRow    │  logs (raw)   │
│  ~10k/sec)   │                 │  transform → buffer →    │                 │      │        │
└──────────────┘                 │  flush (size/time)       │                 │      ▼ MV     │
       │                         │        │ invalid          │                 │  logs_1m      │
       │ /metrics                │        ▼                  │                 │ (Aggregating) │
       ▼                         │   DLQ topic               │                 └───────┬───────┘
  Prometheus ◀── /metrics ───────┘                                                     │
       │                              ┌────────────────┐   ClickHouse queries          │
       ▼                              │  Express API   │ ◀────────────────────────────┘
    Grafana                           │  /reports/*    │
                                      │  + /public UI  │
                                      └────────────────┘
Infra (docker-compose): Kafka (KRaft), ClickHouse, Kafka UI, Prometheus, Grafana.
Node processes (npm scripts): producer, consumer, api — run locally against dockerized infra.
```

---

## Tech Stack

| Concern | Choice |
|---|---|
| Runtime | Node 20+, TypeScript (strict), ESM, `tsx` for dev |
| Kafka client | `kafkajs` |
| ClickHouse driver | `@clickhouse/client` |
| Validation | `zod` |
| HTTP / API | `express` (+ `express.static` for dashboard) |
| Logging | `pino` (+ `pino-pretty` in dev) |
| Metrics | `prom-client` |
| Mock data | `@faker-js/faker` |
| Testing | `vitest` (unit) + `testcontainers` (integration: Kafka + ClickHouse) |
| Env | Node built-in `--env-file=.env` (no `dotenv`), values zod-validated |
| Infra | `docker-compose`: Kafka (KRaft, no ZooKeeper), ClickHouse, Kafka UI, Prometheus, Grafana |

---

## Project Structure

```
.
├── docker-compose.yml
├── .env.example                      # all config keys, safe defaults
├── package.json                      # scripts: dev:*, db:init, up, down
├── tsconfig.json                     # strict, ESM (NodeNext)
├── Makefile                          # convenience targets (up, seed, reports…)
├── clickhouse/
│   ├── init/01_schema.sql            # logs MergeTree table (+ DLQ audit optional)
│   └── init/02_materialized_views.sql# AggregatingMergeTree target + MV
├── prometheus/prometheus.yml         # scrape producer/consumer/api /metrics
├── grafana/provisioning/…            # datasource + dashboard auto-provision
│   └── dashboards/pipeline.json      # throughput, DLQ rate, insert latency, lag
├── public/index.html                 # minimal dashboard (fetches /reports/*)
└── src/
    ├── config/index.ts               # env → zod-parsed typed config
    ├── lib/
    │   ├── logger.ts                 # pino factory
    │   ├── kafka.ts                  # shared KafkaJS client factory
    │   ├── clickhouse.ts             # @clickhouse/client factory
    │   ├── metrics.ts                # prom-client registry + shared metrics
    │   └── metrics-server.ts         # tiny HTTP /metrics endpoint (producer/consumer)
    ├── domain/log-event.ts           # zod schemas + TS types (raw + row)
    ├── producer/index.ts             # high-frequency generator + rate limiter
    ├── consumer/
    │   ├── index.ts                  # eachBatch entrypoint, offset mgmt, shutdown
    │   ├── batch-buffer.ts           # accumulate + flush on size/time
    │   ├── transform.ts              # shape transform (raw → ClickHouse row)
    │   └── dlq.ts                    # publish invalid messages to DLQ topic
    └── api/
        ├── index.ts                  # Express server + static + /metrics
        ├── routes/reports.ts         # report endpoints (express.Router)
        └── queries.ts                # ClickHouse report SQL (uses -Merge)
```

Plus Claude Code config (already created; see Step 10):
```
├── CLAUDE.md                         # canonical project context (single source of truth)
└── .claude/
    ├── settings.json                 # permission allowlist (fewer prompts)
    ├── skills/
    │   ├── run-pipeline/SKILL.md     # operational commands (up/down, load, dev procs)
    │   └── clickhouse-queries/SKILL.md # diagnostic + report SQL reference
    └── agents/
        ├── clickhouse-expert.md      # CH schema/MV/query design subagent
        ├── pipeline-verifier.md      # e2e verification subagent (invokes the skills)
        └── etl-code-reviewer.md      # reviews new TS against streaming invariants
```

Single TypeScript package, three entrypoints (`producer`, `consumer`, `api`) sharing `config`/`lib`/`domain`. Simplest layout that still separates concerns.

---

## Implementation Steps

### 1. Scaffolding & config — DONE
- `package.json` (ESM, `type: module`), `tsconfig.json` (strict, `moduleResolution: NodeNext`).
- Install deps: `kafkajs @clickhouse/client zod express pino prom-client @faker-js/faker`; dev: `tsx typescript @types/node @types/express pino-pretty vitest testcontainers`.
- npm scripts pass `--env-file=.env` to `tsx` (Node 20.6+ native env loading — no `dotenv`).
- `src/config/index.ts`: read `process.env`, validate with zod, export typed `config`. Keys: `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`, `KAFKA_GROUP_ID`, `CLICKHOUSE_URL/DB/USER/PASSWORD`, `BATCH_SIZE`, `FLUSH_INTERVAL_MS`, `PRODUCER_RATE`, `API_PORT`, `PRODUCER_METRICS_PORT`, `CONSUMER_METRICS_PORT`. Provide `.env.example`.
- `src/lib/logger.ts` (pino), `src/lib/metrics.ts` (shared prom-client Registry + counters/histograms/gauges), `src/lib/kafka.ts`, `src/lib/clickhouse.ts` factories.

### 2. Domain model (`src/domain/log-event.ts`)
- `RawLogEventSchema` (zod): `timestamp` (ISO string), `level` (enum), `service`, `host`, `traceId`, `statusCode`, `latencyMs`, `message`. This validates what the producer emits / consumer receives.
- `toRow()` transform → ClickHouse row shape (`timestamp` DateTime64, snake_case columns). Export inferred TS types.

### 3. Infra — `docker-compose.yml` — DONE
- **Kafka** in KRaft mode (`apache/kafka` or `bitnami/kafka`, single broker, no ZooKeeper); auto-create topics off — create topics explicitly in an init step or via app admin on boot.
- **ClickHouse** (`clickhouse/clickhouse-server`), mount `clickhouse/init/*.sql` into `/docker-entrypoint-initdb.d/` for auto-schema; expose HTTP 8123 (Play UI) + native 9000.
- **Kafka UI** (`provectuslabs/kafka-ui` or `redpanda-console`) for topic/consumer-group inspection.
- **Prometheus** (mount `prometheus/prometheus.yml`) scraping the three Node `/metrics` endpoints (host.docker.internal).
- **Grafana** with provisioned Prometheus datasource + pipeline dashboard.
- Healthchecks + `depends_on` so ordering is sane.

### 4. ClickHouse schema (`clickhouse/init/*.sql`)
- **Raw table** `logs`: `MergeTree`, `PARTITION BY toYYYYMMDD(timestamp)`, `ORDER BY (service, level, timestamp)`, `LowCardinality` for level/service/host, `TTL timestamp + INTERVAL 7 DAY` (retention demo).
- **Aggregate target** `logs_1m`: `AggregatingMergeTree`, `ORDER BY (minute, service, level)`, holding `count`, `errors` (status ≥ 500), `quantilesState(0.5,0.95,0.99)(latency_ms)`, `latency_sum`.
- **Materialized View** `logs_1m_mv TO logs_1m`: `SELECT toStartOfMinute(ts) …` — incremental aggregation on every insert (the ClickHouse-idiomatic reporting pattern).

### 5. Producer (`src/producer/index.ts`)
- KafkaJS producer, **idempotent**, `acks=all`, gzip compression.
- Generate mock logs with faker; realistic distribution (mostly INFO, some WARN/ERROR, occasional 5xx, latency spread).
- **Rate limiter** targeting `PRODUCER_RATE` (~10k/sec): send in sub-batches (e.g. 500–1000 msgs) via `sendBatch`, throttle per tick.
- Emit a small % of intentionally malformed events (to exercise the DLQ path).
- Expose `/metrics` (produced count, send errors) via `metrics-server`.
- Graceful shutdown on SIGINT/SIGTERM (flush + disconnect).

### 6. Consumer / ETL (`src/consumer/`)
- KafkaJS consumer, `autoCommit: false`, **`eachBatch`** (not `eachMessage`) for throughput.
- Per message: zod-validate → valid rows into **`batch-buffer`**, invalid → **`dlq`** (publish to DLQ topic + metric), never crash on bad data.
- **`batch-buffer.ts`**: accumulate across batches; flush when `BATCH_SIZE` reached **or** `FLUSH_INTERVAL_MS` timer fires (whichever first) → single bulk `client.insert({ format: 'JSONEachRow' })`.
- **Backpressure**: `pause()` the partition while a flush is in-flight / buffer above high-watermark, `resume()` after; call `heartbeat()` and `resolveOffset()` progressively; `commitOffsetsIfNecessary()` only **after** a successful ClickHouse insert (at-least-once, no data loss).
- **Graceful shutdown**: SIGTERM → stop consuming, flush remaining buffer, commit, disconnect.
- Metrics: consumed/inserted counters, DLQ counter, insert-duration histogram, buffer-size gauge, consumer-lag gauge (via Kafka admin `fetchOffsets`).

### 7. Reporting API + dashboard (`src/api/`)
- Express server; `routes/reports.ts` as an `express.Router`. `queries.ts` reads the **pre-aggregated** `logs_1m` using `-Merge` combinators (`quantilesMerge`) — fast, ClickHouse-idiomatic.
- Endpoints: `GET /reports/throughput` (rows/min), `/reports/errors-by-service`, `/reports/latency-percentiles`, `/reports/top-services`. Query params for time window.
- `express.static('public')` serves `public/index.html`: minimal dashboard that fetches the endpoints and renders simple tables/charts (vanilla JS, no framework).
- Expose `/metrics` and `/health`. Central error-handling middleware for clean JSON errors.

### 8. Metrics wiring (Prometheus + Grafana)
- Each Node process exposes `/metrics` (prom-client). `prometheus.yml` scrapes all three.
- Grafana `pipeline.json` dashboard: ingest throughput, DLQ rate, ClickHouse insert latency (p50/p95), buffer size, consumer lag.

### 9. Developer ergonomics
- npm scripts: `up`/`down` (compose), `db:init` (apply SQL / verify), `dev:producer`, `dev:consumer`, `dev:api`, `dev` (all via `concurrently` or separate terminals). `Makefile` mirrors these.
- Rewrite `README.md` into runnable quickstart: `docker compose up -d` → `npm run db:init` → start consumer → start producer → open API/dashboard/Grafana/Kafka UI.

### 10. Claude Code configuration (DRY, token-lean) — DONE

**Guiding principle:** one fact lives in exactly one place. `CLAUDE.md` holds durable context; skills hold procedures/reference; agents orchestrate by *pointing to* skills. No prose is duplicated across files, and each file is kept terse (bullets/tables/commands, not paragraphs) to minimize tokens loaded per turn.

- **`CLAUDE.md` (root)** — the single source of truth, deliberately compact: project summary, data-flow line, component map, conventions/invariants, and pointers to the two skills. Commands/SQL are *not* copied here.
- **`.claude/settings.json`** — permission allowlist to cut prompts for this project's safe, frequent commands (`docker compose`, `npm run`, `npx tsx`, localhost `curl`). No secrets.
- **`.claude/skills/run-pipeline/SKILL.md`** — the *only* home for operational commands (infra up/down, `db:init`, dev procs, load, endpoints). Everything else references it.
- **`.claude/skills/clickhouse-queries/SKILL.md`** — the *only* home for diagnostic/report SQL (counts, `logs_1m` checks, `quantilesMerge` reports, partition/TTL).
- **`.claude/agents/clickhouse-expert.md`** — CH schema/MV/query design subagent; defers detail to the skills + `clickhouse/init/*.sql`.
- **`.claude/agents/pipeline-verifier.md`** — e2e verification subagent; orchestration only, invokes the two skills, zero duplicated commands/SQL.
- **`.claude/agents/etl-code-reviewer.md`** — reviews new/changed TS against this app's streaming invariants (offset-after-insert, backpressure, event-loop safety, DLQ, shutdown, module boundaries). Enforces CLAUDE.md conventions on the tricky parts; does not restate general TS/Express style.

This structure is the token-saving lesson: `CLAUDE.md` loads every turn so it stays small; heavier procedural/reference detail lives in skills that load **on demand**; agents run in isolated context and pull only the skill(s) they need.

### 11. Tests (`vitest` + `testcontainers`)
- **Unit** (`*.test.ts`, no infra): `domain/log-event` validation (good/malformed), `consumer/transform` (raw → row shape), `consumer/batch-buffer` (flush on size, flush on timer, drain-on-shutdown).
- **Integration** (`testcontainers`, tagged/separate script): spin up Kafka + ClickHouse containers → produce a small fixed batch (incl. malformed) → run the consumer path → assert rows land in `logs`, the MV populates `logs_1m`, and a malformed record hit the DLQ topic. Reuses the same SQL as the `clickhouse-queries` skill (no duplication).
- npm scripts: `test` (unit), `test:integration` (containers). Kept fast/deterministic; integration uses tiny batches, not the 10k/sec load.

---

## Best-practices demonstrated (learning goals)
- **Kafka:** idempotent producer, compression, `sendBatch`; consumer `eachBatch`, manual offset commit after sink success, heartbeats, consumer-group inspection, DLQ pattern.
- **ClickHouse:** columnar `MergeTree` design (partitioning, `ORDER BY`, `LowCardinality`, TTL), **bulk `JSONEachRow` inserts**, incremental **Materialized Views** with `AggregatingMergeTree` + quantile states — pushing aggregation into the DB instead of the app.
- **Node:** never block the event loop, bounded-memory batching with size+time flush, backpressure via pause/resume, graceful shutdown/draining, typed config + runtime validation, structured logging, first-class metrics.

---

## Verification (end-to-end)

0. `npm test` (unit) passes; `npm run test:integration` spins up containers and the ingest→query→DLQ assertions pass.
1. `docker compose up -d` → confirm all containers healthy (`docker compose ps`); Kafka UI reachable, ClickHouse Play (`http://localhost:8123/play`) reachable, Grafana up.
2. `npm run db:init` → verify `logs`, `logs_1m`, `logs_1m_mv` exist (`SHOW TABLES`).
3. Start consumer, then producer. Watch consumer logs: batches flushing, DLQ counts for malformed events.
4. In ClickHouse Play: `SELECT count() FROM logs` climbs into the hundreds of thousands within seconds; `SELECT * FROM logs_1m LIMIT 5` shows aggregated rows populated by the MV.
5. Hit the API: `curl localhost:$API_PORT/reports/throughput` and open `public/index.html` — dashboard renders live numbers.
6. Kafka UI: confirm the DLQ topic has the intentionally-malformed messages and the consumer group lag stays bounded (backpressure working).
7. Grafana: pipeline dashboard shows throughput, insert latency, DLQ rate, buffer size, lag.
8. **Reliability checks:** kill the consumer mid-stream (SIGTERM) → logs show buffer drain + offset commit; restart → no duplicate-driven crash, ingestion resumes from committed offset. Stop the producer → buffer flushes remaining rows on the time-based flush.

---

## Open follow-ups (not in initial build)
CI workflow (GitHub Actions: typecheck + unit + integration) and ESLint were deferred — easy to add later on top of this structure.
