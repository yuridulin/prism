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
    ts      DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    tag_id  UInt32 CODEC(Delta(4), ZSTD(1)),
    value   Float32 CODEC(Gorilla, LZ4),
    quality UInt16 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (tag_id, ts)
TTL toDateTime(ts) + INTERVAL 400 DAY
SETTINGS index_granularity = 8192;
