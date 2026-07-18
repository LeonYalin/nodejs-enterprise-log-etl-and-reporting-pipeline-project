---
name: run-pipeline
description: Operational commands for the log ETL pipeline — start/stop infra, init ClickHouse schema, run the producer/consumer/api, generate load, find the UIs and metrics endpoints. Use whenever running, operating, or load-testing this project locally.
---

# Run the pipeline

Prereqs: Docker running · `npm install` done · `.env` present (copy from `.env.example`).

## Infra (docker-compose)
- Up: `docker compose up -d`
- Status / health: `docker compose ps`
- Logs: `docker compose logs -f <service>`
- Down (keep data): `docker compose down` — Down + wipe volumes: `docker compose down -v`

## Initialize ClickHouse schema
`npm run db:init`   # applies clickhouse/init/*.sql; verifies logs / logs_1m / logs_1m_mv exist

## Run Node processes (separate terminals)
- Consumer / ETL: `npm run dev:consumer`
- Producer (load): `npm run dev:producer`
- Reporting API + dashboard: `npm run dev:api`
- All at once: `npm run dev`

## Tune load
Producer rate via env (rows/sec): `PRODUCER_RATE=10000 npm run dev:producer`

## Endpoints (defaults)
- API + reports: `http://localhost:$API_PORT` — `/reports/throughput`, `/reports/errors-by-service`, `/reports/latency-percentiles`, `/reports/top-services`
- Dashboard: `http://localhost:$API_PORT/` (serves `public/index.html`)
- Metrics: producer `:$PRODUCER_METRICS_PORT/metrics`, consumer `:$CONSUMER_METRICS_PORT/metrics`, api `/metrics`
- Kafka UI: `http://localhost:8080` · ClickHouse Play: `http://localhost:8123/play` · Prometheus: `http://localhost:9090` · Grafana: `http://localhost:3000`

## Shutdown
`Ctrl-C` / SIGTERM drains the buffer, commits offsets, and disconnects — safe to stop anytime.

## Tests
`npm test` (unit) · `npm run test:integration` (spins up Kafka + ClickHouse via testcontainers).
