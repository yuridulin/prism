package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"prism/go-api/internal/model"
)

type Timescale struct {
	pool *pgxpool.Pool
}

func NewTimescale(dsn string) (*Timescale, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 16
	cfg.MinConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return &Timescale{pool: pool}, nil
}

func (s *Timescale) Name() string { return "timescaledb" }

func (s *Timescale) Close() error {
	s.pool.Close()
	return nil
}

func (s *Timescale) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Archive max gap is 1h; 3h still finds the previous minute/hour point at 364d ago.
const tsLocfLookback = 3 * time.Hour

const tsLocfSQL = `
	SELECT s.ts, s.tag_id, s.value, s.quality
	FROM unnest($1::int4[]) AS t(tag_id)
	CROSS JOIN LATERAL (
		SELECT ts, tag_id, value, quality
		FROM samples
		WHERE samples.tag_id = t.tag_id AND ts <= $2 AND ts >= $3
		ORDER BY ts DESC
		LIMIT 1
	) s`

const tsLocfUnboundedSQL = `
	SELECT s.ts, s.tag_id, s.value, s.quality
	FROM unnest($1::int4[]) AS t(tag_id)
	CROSS JOIN LATERAL (
		SELECT ts, tag_id, value, quality
		FROM samples
		WHERE samples.tag_id = t.tag_id AND ts <= $2
		ORDER BY ts DESC
		LIMIT 1
	) s`

const tsRangeSQL = `
	SELECT ts, tag_id, value, quality
	FROM samples
	WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
	ORDER BY tag_id, ts`

func (s *Timescale) Write(ctx context.Context, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"samples"},
		[]string{"ts", "tag_id", "value", "quality"},
		&sampleCopySrc{samples: samples, idx: -1, row: make([]any, 4)},
	)
	return err
}

type sampleCopySrc struct {
	samples []model.Sample
	idx     int
	row     []any
}

func (s *sampleCopySrc) Next() bool {
	s.idx++
	return s.idx < len(s.samples)
}

func (s *sampleCopySrc) Values() ([]any, error) {
	p := &s.samples[s.idx]
	s.row[0] = p.TS.UTC()
	s.row[1] = int32(p.TagID)
	s.row[2] = float32(p.Value)
	s.row[3] = int16(p.Quality)
	return s.row, nil
}

func (s *sampleCopySrc) Err() error { return nil }

func (s *Timescale) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
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

func (s *Timescale) locf(ctx context.Context, tagIDs []uint32, at time.Time, bounded bool) ([]model.Sample, error) {
	at = at.UTC()
	var (
		rows pgx.Rows
		err  error
	)
	if bounded {
		rows, err = s.pool.Query(ctx, tsLocfSQL, intTags(tagIDs), at, at.Add(-tsLocfLookback))
	} else {
		rows, err = s.pool.Query(ctx, tsLocfUnboundedSQL, intTags(tagIDs), at)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSamples(rows, false)
}

func (s *Timescale) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	head, err := s.Locf(ctx, tagIDs, from)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, tsRangeSQL, intTags(tagIDs), from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tail, err := scanSamples(rows, false)
	if err != nil {
		return nil, err
	}
	return mergeCHRange(tagIDs, head, tail), nil
}

func (s *Timescale) UpsertTags(ctx context.Context, tags []model.Tag) error {
	batch := &pgx.Batch{}
	for _, t := range tags {
		batch.Queue(`
			INSERT INTO tags (id, name, unit) VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, unit = EXCLUDED.unit`,
			int32(t.ID), t.Name, t.Unit)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range tags {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Timescale) ListTags(ctx context.Context) ([]model.Tag, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, COALESCE(unit, '') FROM tags ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Tag
	for rows.Next() {
		var t model.Tag
		var id int32
		if err := rows.Scan(&id, &t.Name, &t.Unit); err != nil {
			return nil, err
		}
		t.ID = uint32(id)
		out = append(out, t)
	}
	return out, rows.Err()
}

func intTags(ids []uint32) []int32 {
	out := make([]int32, len(ids))
	for i, id := range ids {
		out[i] = int32(id)
	}
	return out
}

func scanSamples(rows pgx.Rows, carried bool) ([]model.Sample, error) {
	out := make([]model.Sample, 0, 16)
	for rows.Next() {
		var s model.Sample
		var tag int32
		var q int16
		if err := rows.Scan(&s.TS, &tag, &s.Value, &q); err != nil {
			return nil, err
		}
		s.TagID = uint32(tag)
		s.Quality = uint16(q)
		s.Carried = carried
		out = append(out, s)
	}
	return out, rows.Err()
}

