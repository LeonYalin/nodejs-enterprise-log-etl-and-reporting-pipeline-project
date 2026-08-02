import type { ClickHouseClient } from '@clickhouse/client';

export interface TimeParam {
  minutes?: number;
}

/** Parses a time window, defaulting to the past 15 minutes for invalid/missing input. */
export function getIntervalValue(minutes?: number): number {
  const m = Number(minutes);
  return !isNaN(m) && m > 0 ? m : 15;
}

/**
 * Report queries over the pre-aggregated logs_1m materialized view.
 *
 * The client is injected so the suite can point these at a throwaway container
 * instead of the process-wide singleton.
 */
export function createReportQueries(client: ClickHouseClient) {
  return {
    /**
     * Throughput: Total valid logs ingested per minute over the time window.
     */
    async getThroughput({ minutes }: TimeParam) {
      const interval = getIntervalValue(minutes);
      const query = `
        SELECT
          minute as time,
          sum(total_count) as count
        FROM logs_1m
        WHERE minute >= now() - INTERVAL {interval:UInt32} MINUTE
        GROUP BY time
        ORDER BY time ASC
      `;
      const resultSet = await client.query({ query, query_params: { interval }, format: 'JSONEachRow' });
      return resultSet.json();
    },

    /**
     * Errors by Service: sums the MV's precomputed error_count (status_code >= 500).
     */
    async getErrorsByService({ minutes }: TimeParam) {
      const interval = getIntervalValue(minutes);
      const query = `
        SELECT
          service,
          sum(error_count) as error_count
        FROM logs_1m
        WHERE minute >= now() - INTERVAL {interval:UInt32} MINUTE
        GROUP BY service
        ORDER BY error_count DESC
      `;
      const resultSet = await client.query({ query, query_params: { interval }, format: 'JSONEachRow' });
      return resultSet.json();
    },

    /**
     * Latency Percentiles: Merges the stateful quantiles array for p50, p90, and p99
     */
    async getLatencyPercentiles({ minutes }: TimeParam) {
      const interval = getIntervalValue(minutes);
      const query = `
        SELECT
          service,
          quantilesMerge(0.50, 0.90, 0.99)(latency_quantiles) as percentiles
        FROM logs_1m
        WHERE minute >= now() - INTERVAL {interval:UInt32} MINUTE
        GROUP BY service
        ORDER BY service ASC
      `;
      // Returns fields: service, percentiles: [p50, p90, p99]
      const resultSet = await client.query({ query, query_params: { interval }, format: 'JSONEachRow' });
      return resultSet.json();
    },

    /**
     * Top Services: Absolute transaction/volume share by message volume
     */
    async getTopServices({ minutes }: TimeParam) {
      const interval = getIntervalValue(minutes);
      const query = `
        SELECT
          service,
          sum(total_count) as volume
        FROM logs_1m
        WHERE minute >= now() - INTERVAL {interval:UInt32} MINUTE
        GROUP BY service
        ORDER BY volume DESC
        LIMIT 10
      `;
      const resultSet = await client.query({ query, query_params: { interval }, format: 'JSONEachRow' });
      return resultSet.json();
    },
  };
}

export type ReportQueries = ReturnType<typeof createReportQueries>;
