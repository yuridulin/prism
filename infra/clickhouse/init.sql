CREATE DATABASE IF NOT EXISTS prism;

CREATE TABLE IF NOT EXISTS prism.points
(
    ts     DateTime64(3, 'UTC'),
    metric LowCardinality(String),
    value  Float64,
    labels Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (metric, ts)
TTL toDateTime(ts) + INTERVAL 7 DAY;
