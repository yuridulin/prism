package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func (s *Timescale) Write(ctx context.Context, points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	const q = `INSERT INTO points (ts, metric, value, labels) VALUES ($1, $2, $3, $4)`
	for _, p := range points {
		raw, err := json.Marshal(model.NormalizeLabels(p.Labels))
		if err != nil {
			return err
		}
		batch.Queue(q, p.TS.UTC(), p.Metric, p.Value, raw)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range points {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Timescale) Query(ctx context.Context, q model.Query) (*model.QueryResult, error) {
	labels, err := json.Marshal(model.NormalizeLabels(q.Labels))
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`
		SELECT time_bucket($1::interval, ts) AS bucket, %s(value) AS value
		FROM points
		WHERE metric = $2 AND ts >= $3 AND ts < $4 AND labels @> $5::jsonb
		GROUP BY bucket
		ORDER BY bucket`, aggSQL(q.Agg))

	rows, err := s.pool.Query(ctx, sql, interval(q.Step), q.Metric, q.From.UTC(), q.To.UTC(), labels)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := &model.QueryResult{Metric: q.Metric, Agg: q.Agg, Step: q.Step.String(), Points: []model.Sample{}}
	for rows.Next() {
		var sample model.Sample
		if err := rows.Scan(&sample.TS, &sample.Value); err != nil {
			return nil, err
		}
		out.Points = append(out.Points, sample)
	}
	return out, rows.Err()
}

func (s *Timescale) Latest(ctx context.Context, metric string, labels map[string]string) (*model.Point, error) {
	raw, err := json.Marshal(model.NormalizeLabels(labels))
	if err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx, `
		SELECT ts, metric, value, labels
		FROM points
		WHERE metric = $1 AND labels @> $2::jsonb
		ORDER BY ts DESC
		LIMIT 1`, metric, raw)

	var p model.Point
	var blob []byte
	if err := row.Scan(&p.TS, &p.Metric, &p.Value, &blob); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(blob, &p.Labels); err != nil {
		return nil, err
	}
	return &p, nil
}

func interval(d time.Duration) string {
	if d < time.Second {
		d = time.Second
	}
	return fmt.Sprintf("%d seconds", int(d.Seconds()))
}
