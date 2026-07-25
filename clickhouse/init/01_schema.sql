-- Create the database context explicitly
CREATE DATABASE IF NOT EXISTS log_pipeline;

-- Switch to the database context
USE log_pipeline;

-- Raw logs ingestion table optimized for high-throughput stream writes
CREATE TABLE IF NOT EXISTS logs (
    -- LowCardinality compresses repetitive strings into numeric dictionaries (huge memory savings)
    service LowCardinality(String),
    level LowCardinality(String),
    host LowCardinality(String),

    -- Core payload fields
    message String,
    trace_id String,
    status_code UInt16,
    latency_ms UInt32,

    -- DateTime64 handles sub-second precision (millisecond resolution)
    timestamp DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
-- Partitions split physical data folders by day for rapid drops or file movements
PARTITION BY toYYYYMMDD(timestamp)
-- Index primary key: sorted from lowest cardinality to highest for binary seek speed
ORDER BY (service, level, timestamp)
-- Auto-delete engine data older than 7 days to manage storage footprints
TTL timestamp + INTERVAL 7 DAY;
