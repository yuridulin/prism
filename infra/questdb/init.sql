CREATE TABLE IF NOT EXISTS samples (
    ts TIMESTAMP,
    tag_id SYMBOL CAPACITY 256 CACHE INDEX,
    value FLOAT,
    quality SHORT
) timestamp(ts) PARTITION BY DAY WAL;

CREATE TABLE IF NOT EXISTS tags (
    id INT,
    name SYMBOL,
    unit SYMBOL
);
