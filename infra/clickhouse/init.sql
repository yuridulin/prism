CREATE DATABASE IF NOT EXISTS prism;

CREATE TABLE IF NOT EXISTS prism.tags
(
    id   UInt32,
    name String,
    unit String
)
ENGINE = ReplacingMergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS prism.samples
(
    ts      DateTime64(3, 'UTC'),
    tag_id  UInt32,
    value   Float32,
    quality UInt16
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (tag_id, ts)
TTL toDateTime(ts) + INTERVAL 7 DAY;
