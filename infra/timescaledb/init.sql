CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tags (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    unit  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS samples (
    ts      TIMESTAMPTZ NOT NULL,
    tag_id  INTEGER     NOT NULL,
    value   REAL        NOT NULL,
    quality SMALLINT    NOT NULL
);

SELECT create_hypertable(
    'samples',
    'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Queries always filter tag_id; the default ts-only index produces seq/ts
-- scans that ignore it. Drop it so (tag_id, ts DESC) is the access path.
DROP INDEX IF EXISTS samples_ts_idx;
CREATE INDEX IF NOT EXISTS samples_tag_ts_idx ON samples (tag_id, ts DESC);

ALTER TABLE samples SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'tag_id',
    timescaledb.compress_orderby = 'ts DESC'
);

-- Seed writes a year in ~1 minute. A 12h job would leave the archive
-- uncompressed for query-mix. Compress closed chunks every 10s.
SELECT add_compression_policy(
    'samples',
    compress_after => INTERVAL '12 hours',
    schedule_interval => INTERVAL '10 seconds',
    if_not_exists => TRUE
);
SELECT add_retention_policy('samples', INTERVAL '400 days', if_not_exists => TRUE);

-- 30d × 8 frequent tags ≈ 345k rows; default work_mem (~1MB) sorts on disk.
ALTER DATABASE prism SET work_mem = '16MB';
ALTER DATABASE prism SET random_page_cost = 1.1;

-- timescaledb-tune otherwise keeps ~1GB WAL (min 512MB) after a fast seed.
ALTER SYSTEM SET max_wal_size = '192MB';
ALTER SYSTEM SET min_wal_size = '32MB';
SELECT pg_reload_conf();
