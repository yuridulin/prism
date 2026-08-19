package store

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"prism/go-api/internal/model"
)

type ClickHouse struct {
	conn driver.Conn
}

func NewClickHouse(dsn string) (*ClickHouse, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	opts.MaxOpenConns = 16
	opts.DialTimeout = 5 * time.Second
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouse{conn: conn}, nil
}

func (s *ClickHouse) Name() string { return "clickhouse" }

func (s *ClickHouse) Close() error { return s.conn.Close() }

func (s *ClickHouse) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

func (s *ClickHouse) Write(ctx context.Context, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO samples (ts, tag_id, value, quality)")
	if err != nil {
		return err
	}
	for _, p := range samples {
		if err := batch.Append(p.TS.UTC(), p.TagID, float32(p.Value), p.Quality); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *ClickHouse) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT ts, tag_id, value, quality
		FROM samples
		WHERE tag_id IN ? AND ts <= ?
		ORDER BY tag_id, ts DESC
		LIMIT 1 BY tag_id`, tagIDs, at.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCH(rows, false)
}

func (s *ClickHouse) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT ts, tag_id, value, quality, carried FROM (
			SELECT ts, tag_id, value, quality, 1 AS carried
			FROM samples
			WHERE tag_id IN ? AND ts <= ?
			ORDER BY tag_id, ts DESC
			LIMIT 1 BY tag_id
			UNION ALL
			SELECT ts, tag_id, value, quality, 0
			FROM samples
			WHERE tag_id IN ? AND ts > ? AND ts <= ?
		)
		ORDER BY tag_id, ts`, tagIDs, from.UTC(), tagIDs, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCHCarried(rows)
}

func (s *ClickHouse) UpsertTags(ctx context.Context, tags []model.Tag) error {
	if len(tags) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO tags (id, name, unit)")
	if err != nil {
		return err
	}
	for _, t := range tags {
		if err := batch.Append(t.ID, t.Name, t.Unit); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *ClickHouse) ListTags(ctx context.Context) ([]model.Tag, error) {
	rows, err := s.conn.Query(ctx, `SELECT id, name, unit FROM tags ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Tag
	for rows.Next() {
		var t model.Tag
		if err := rows.Scan(&t.ID, &t.Name, &t.Unit); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanCH(rows driver.Rows, carried bool) ([]model.Sample, error) {
	var out []model.Sample
	for rows.Next() {
		var s model.Sample
		var value float32
		if err := rows.Scan(&s.TS, &s.TagID, &value, &s.Quality); err != nil {
			return nil, err
		}
		s.Value = float64(value)
		s.Carried = carried
		out = append(out, s)
	}
	return out, rows.Err()
}

func scanCHCarried(rows driver.Rows) ([]model.Sample, error) {
	var out []model.Sample
	for rows.Next() {
		var s model.Sample
		var value float32
		var carried uint8
		if err := rows.Scan(&s.TS, &s.TagID, &value, &s.Quality, &carried); err != nil {
			return nil, err
		}
		s.Value = float64(value)
		s.Carried = carried != 0
		out = append(out, s)
	}
	return out, rows.Err()
}

