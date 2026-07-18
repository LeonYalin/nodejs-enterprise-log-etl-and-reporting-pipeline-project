---
name: clickhouse-queries
description: Diagnostic and reporting SQL for the ClickHouse side of the log pipeline — row counts, ingest sanity checks, materialized-view aggregates, latency percentiles, partition/TTL checks. Use when inspecting pipeline data or writing report queries.
---

# ClickHouse queries

Run in ClickHouse Play (`http://localhost:8123/play`) or over HTTP:
`curl -s 'http://localhost:8123/' --data-binary "SELECT count() FROM logs"`

## Sanity / ingest
- Tables exist: `SHOW TABLES`
- Raw count: `SELECT count() FROM logs`
- Recent rows: `SELECT * FROM logs ORDER BY ts DESC LIMIT 10`
- Ingest in last minute: `SELECT count() FROM logs WHERE ts > now() - INTERVAL 1 MINUTE`

## Materialized view populated
- `SELECT * FROM logs_1m ORDER BY minute DESC LIMIT 5`
- Raw ≈ aggregated: `SELECT sum(count) FROM logs_1m` should track `SELECT count() FROM logs`

## Reports (read the aggregate, use -Merge)
- Throughput/min: `SELECT minute, sum(count) AS c FROM logs_1m GROUP BY minute ORDER BY minute DESC LIMIT 30`
- Errors by service: `SELECT service, sum(errors) AS e, sum(count) AS n FROM logs_1m GROUP BY service ORDER BY e DESC`
- Latency percentiles: `SELECT service, quantilesMerge(0.5, 0.95, 0.99)(latency_quantiles) FROM logs_1m GROUP BY service`

## Ops
- Partitions: `SELECT partition, sum(rows) FROM system.parts WHERE table = 'logs' AND active GROUP BY partition`
- Retention window: `SELECT min(ts), max(ts) FROM logs`
