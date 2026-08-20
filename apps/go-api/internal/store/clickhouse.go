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
	opts.MaxIdleConns = 16
	opts.DialTimeout = 5 * time.Second
	opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
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
	if err := s.insert(ctx, samples); err != nil {
		return s.insert(ctx, samples)
	}
	return nil
}

func (s *ClickHouse) insert(ctx context.Context, samples []model.Sample) error {
	// Short busy window coalesces the 8 write-ceiling workers into fewer parts
	// without parking each HTTP write for the default 200ms flush.
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":                 1,
		"wait_for_async_insert":        1,
		"async_insert_busy_timeout_ms": 10,
	}))
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO samples (ts, tag_id, value, quality)")
	if err != nil {
		return err
	}
	for i := range samples {
		p := &samples[i]
		if err := batch.Append(p.TS.UTC(), p.TagID, float32(p.Value), p.Quality); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

const chLocfLookback = 48 * time.Hour

const chLocfSQL = `
		SELECT s.ts, s.tag_id, s.value, s.quality
		FROM samples AS s
		WHERE (s.tag_id, s.ts) IN (
			SELECT t.tag_id, max(t.ts)
			FROM samples AS t
			WHERE t.tag_id IN ? AND t.ts <= ?
			GROUP BY t.tag_id
		)`

const chLocfBoundedSQL = `
		SELECT s.ts, s.tag_id, s.value, s.quality
		FROM samples AS s
		WHERE (s.tag_id, s.ts) IN (
			SELECT t.tag_id, max(t.ts)
			FROM samples AS t
			WHERE t.tag_id IN ? AND t.ts <= ? AND t.ts >= ?
			GROUP BY t.tag_id
		)`

func (s *ClickHouse) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	out, err := s.locf(ctx, tagIDs, at, true)
	if err != nil {
		return nil, err
	}
	missing := missingTagIDs(tagIDs, out)
	if len(missing) == 0 {
		return out, nil
	}
	rest, err := s.locf(ctx, missing, at, false)
	if err != nil {
		return nil, err
	}
	return append(out, rest...), nil
}

func (s *ClickHouse) locf(ctx context.Context, tagIDs []uint32, at time.Time, bounded bool) ([]model.Sample, error) {
	at = at.UTC()
	var (
		rows driver.Rows
		err  error
	)
	if bounded {
		rows, err = s.conn.Query(ctx, chLocfBoundedSQL, tagIDs, at, at.Add(-chLocfLookback))
	} else {
		rows, err = s.conn.Query(ctx, chLocfSQL, tagIDs, at)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCH(rows, false)
}

func (s *ClickHouse) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	head, err := s.Locf(ctx, tagIDs, from)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Query(ctx, `
		SELECT s.ts, s.tag_id, s.value, s.quality
		FROM samples AS s
		WHERE s.tag_id IN ? AND s.ts > ? AND s.ts <= ?
		ORDER BY s.tag_id, s.ts`, tagIDs, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tail, err := scanCH(rows, false)
	if err != nil {
		return nil, err
	}
	return mergeCHRange(tagIDs, head, tail), nil
}

func missingTagIDs(tagIDs []uint32, rows []model.Sample) []uint32 {
	found := make(map[uint32]struct{}, len(rows))
	for i := range rows {
		found[rows[i].TagID] = struct{}{}
	}
	var missing []uint32
	seen := make(map[uint32]struct{}, len(tagIDs))
	for _, id := range tagIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func mergeCHRange(tagIDs []uint32, head, tail []model.Sample) []model.Sample {
	buckets := make(map[uint32][]model.Sample, len(tagIDs))
	for _, id := range tagIDs {
		buckets[id] = nil
	}
	for i := range head {
		head[i].Carried = true
		buckets[head[i].TagID] = append(buckets[head[i].TagID], head[i])
	}
	var extra []model.Sample
	for i := range tail {
		id := tail[i].TagID
		if _, ok := buckets[id]; ok {
			buckets[id] = append(buckets[id], tail[i])
		} else {
			extra = append(extra, tail[i])
		}
	}
	out := make([]model.Sample, 0, len(head)+len(tail))
	for _, id := range tagIDs {
		out = append(out, buckets[id]...)
	}
	return append(out, extra...)
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
			_ = batch.Abort()
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
