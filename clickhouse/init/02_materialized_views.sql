USE log_pipeline;

-- Target storage container for pre-aggregated minute rollups
CREATE TABLE IF NOT EXISTS logs_1m (
    minute DateTime('UTC'),
    service LowCardinality(String),
    level LowCardinality(String),

    -- Simple state counters
    total_count SimpleAggregateFunction(sum, UInt64),
    error_count SimpleAggregateFunction(sum, UInt64),

    -- High-performance mathematical quantile state allocations
    -- Stores intermediate t-digest states rather than raw final numbers
    latency_quantiles AggregateFunction(quantiles(0.5, 0.95, 0.99), UInt32),
    latency_sum SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (minute, service, level);

-- The streaming automation bridge that calculates aggregations mid-flight
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_1m_mv
TO logs_1m AS
SELECT
    toStartOfMinute(timestamp) AS minute,
    service,
    level,
    -- count() maps directly to sum aggregation via the target table engine rules
    count() AS total_count,
    -- Count errors where HTTP status codes indicate failures (>= 500)
    sum(if(status_code >= 500, 1, 0)) AS error_count,
    -- Capture the raw quantile state distribution profile for later merging
    quantilesState(0.5, 0.95, 0.99)(latency_ms) AS latency_quantiles,
    sum(latency_ms) AS latency_sum
FROM logs
GROUP BY minute, service, level;
