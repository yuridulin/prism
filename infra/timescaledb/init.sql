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

SELECT create_hypertable('samples', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS samples_tag_ts_idx ON samples (tag_id, ts DESC);

ALTER TABLE samples SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'tag_id'
);

SELECT add_compression_policy('samples', INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('samples', INTERVAL '7 days', if_not_exists => TRUE);
