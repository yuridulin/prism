CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS points (
    ts      TIMESTAMPTZ       NOT NULL,
    metric  TEXT              NOT NULL,
    value   DOUBLE PRECISION  NOT NULL,
    labels  JSONB             NOT NULL DEFAULT '{}'::jsonb
);

SELECT create_hypertable('points', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS points_metric_ts_idx ON points (metric, ts DESC);
CREATE INDEX IF NOT EXISTS points_labels_gin_idx ON points USING GIN (labels);

ALTER TABLE points SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'metric'
);

SELECT add_compression_policy('points', INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('points', INTERVAL '7 days', if_not_exists => TRUE);
