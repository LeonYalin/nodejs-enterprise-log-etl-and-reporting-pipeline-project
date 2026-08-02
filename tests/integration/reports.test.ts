import { describe, it, expect, beforeAll, afterAll, inject } from 'vitest';
import request from 'supertest';
import type { ClickHouseClient } from '@clickhouse/client';
import { createApp } from '../../src/api/app.js';
import type { ClickHouseLogRow } from '../../src/domain/log-event.js';
import { createTestClickHouseClient, waitFor } from './helpers.js';

const connection = inject('clickhouse');

function row(overrides: Partial<ClickHouseLogRow>): ClickHouseLogRow {
  return {
    timestamp: new Date().toISOString(),
    level: 'INFO',
    service: 'auth-service',
    host: 'host-01',
    trace_id: 'trace',
    status_code: 200,
    latency_ms: 20,
    message: 'ok',
    ...overrides,
  };
}

// 5 auth (ok), 3 payment (all 5xx), 2 router (ok) = 10 rows.
const seedRows: ClickHouseLogRow[] = [
  ...Array.from({ length: 5 }, (_, i) => row({ service: 'auth-service', latency_ms: 10 + i })),
  ...Array.from({ length: 3 }, () => row({ service: 'payment-gateway', level: 'ERROR', status_code: 503, latency_ms: 1000 })),
  ...Array.from({ length: 2 }, () => row({ service: 'api-router', latency_ms: 30 })),
];

let client: ClickHouseClient;
let app: ReturnType<typeof createApp>;

describe('reporting API against a live ClickHouse', () => {
  beforeAll(async () => {
    client = createTestClickHouseClient(connection);
    app = createApp({ client });

    await client.command({ query: 'TRUNCATE TABLE logs' });
    await client.command({ query: 'TRUNCATE TABLE logs_1m' });
    await client.insert({ table: 'logs', values: seedRows, format: 'JSONEachRow' });

    // The materialized view fills logs_1m on insert; wait for it to land.
    await waitFor('logs_1m to be populated by the materialized view', async () => {
      const result = await client.query({ query: 'SELECT sum(total_count) AS c FROM logs_1m', format: 'JSONEachRow' });
      const [ first ] = await result.json<{ c: string }>();
      return Number(first?.c ?? 0) === seedRows.length;
    });
  });

  afterAll(async () => {
    await client?.close();
  });

  it('reports the database as connected on /health', async () => {
    const response = await request(app).get('/health').expect(200);

    expect(response.body).toMatchObject({ status: 'UP', database: 'CONNECTED' });
  });

  it('exposes Prometheus metrics', async () => {
    const response = await request(app).get('/metrics').expect(200);

    expect(response.text).toContain('pipeline_messages_consumed_total');
  });

  it('sums throughput from the pre-aggregated view', async () => {
    const response = await request(app).get('/reports/throughput').expect(200);

    expect(response.body.success).toBe(true);
    const total = response.body.data.reduce((sum: number, r: { count: string }) => sum + Number(r.count), 0);
    expect(total).toBe(10);
  });

  it('attributes errors to the right service using the MV error_count', async () => {
    const response = await request(app).get('/reports/errors-by-service').expect(200);

    const errorsByService = Object.fromEntries(
      response.body.data.map((r: { service: string; error_count: string }) => [ r.service, Number(r.error_count) ]),
    );

    expect(errorsByService['payment-gateway']).toBe(3);
    expect(errorsByService['auth-service']).toBe(0);
    expect(errorsByService['api-router']).toBe(0);
  });

  it('ranks top services by volume', async () => {
    const response = await request(app).get('/reports/top-services').expect(200);

    expect(response.body.data.map((r: { service: string }) => r.service)).toEqual([
      'auth-service', 'payment-gateway', 'api-router',
    ]);
    expect(Number(response.body.data[0].volume)).toBe(5);
  });

  it('merges quantile states into per-service percentiles', async () => {
    const response = await request(app).get('/reports/latency-percentiles').expect(200);

    const byService = Object.fromEntries(
      response.body.data.map((r: { service: string; percentiles: unknown[] }) => [ r.service, r.percentiles ]),
    );

    // quantilesMerge(0.50, 0.90, 0.99) -> three values per service.
    expect(byService['payment-gateway']).toHaveLength(3);
    // Every payment-gateway row had latency 1000, so all quantiles collapse to it.
    expect(byService['payment-gateway'].map(Number)).toEqual([ 1000, 1000, 1000 ]);
    // auth-service latencies were 10..14, well below the error service.
    expect(Number(byService['auth-service'][0])).toBeLessThan(100);
  });

  it('honours the minutes window parameter', async () => {
    const response = await request(app).get('/reports/top-services?minutes=60').expect(200);

    expect(Number(response.body.data[0].volume)).toBe(5);
  });

  it('falls back to the default window for a nonsense minutes value', async () => {
    const response = await request(app).get('/reports/throughput?minutes=abc').expect(200);

    // Number('abc') is NaN -> getIntervalValue falls back to 15 minutes, which
    // still covers the freshly seeded rows.
    const total = response.body.data.reduce((sum: number, r: { count: string }) => sum + Number(r.count), 0);
    expect(total).toBe(10);
  });
});
