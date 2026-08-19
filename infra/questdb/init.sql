CREATE TABLE IF NOT EXISTS samples (
    ts TIMESTAMP,
    tag_id INT,
    value FLOAT,
    quality SHORT
) timestamp(ts) PARTITION BY DAY WAL;

CREATE TABLE IF NOT EXISTS tags (
    id INT,
    name SYMBOL,
    unit SYMBOL
);
