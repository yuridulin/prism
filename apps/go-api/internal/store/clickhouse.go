package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

func (s *ClickHouse) Write(ctx context.Context, points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO points (ts, metric, value, labels)")
	if err != nil {
		return err
	}
	for _, p := range points {
		if err := batch.Append(p.TS.UTC(), p.Metric, p.Value, model.NormalizeLabels(p.Labels)); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *ClickHouse) Query(ctx context.Context, q model.Query) (*model.QueryResult, error) {
	conds, args := labelFilters(q.Labels)
	sql := fmt.Sprintf(`
		SELECT toStartOfInterval(ts, INTERVAL %d SECOND) AS bucket, %s(value) AS value
		FROM points
		WHERE metric = ? AND ts >= ? AND ts < ? %s
		GROUP BY bucket
		ORDER BY bucket`, max(int(q.Step.Seconds()), 1), aggSQL(q.Agg), conds)

	args = append([]any{q.Metric, q.From.UTC(), q.To.UTC()}, args...)
	rows, err := s.conn.Query(ctx, sql, args...)
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

func (s *ClickHouse) Latest(ctx context.Context, metric string, labels map[string]string) (*model.Point, error) {
	conds, args := labelFilters(labels)
	sql := fmt.Sprintf(`
		SELECT ts, metric, value, labels
		FROM points
		WHERE metric = ? %s
		ORDER BY ts DESC
		LIMIT 1`, conds)
	args = append([]any{metric}, args...)

	var p model.Point
	if err := s.conn.QueryRow(ctx, sql, args...).Scan(&p.TS, &p.Metric, &p.Value, &p.Labels); err != nil {
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no rows") {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &p, nil
}

func labelFilters(labels map[string]string) (string, []any) {
	if len(labels) == 0 {
		return "", nil
	}
	var b strings.Builder
	args := make([]any, 0, len(labels))
	for k, v := range labels {
		b.WriteString(" AND labels[?] = ?")
		args = append(args, k, v)
	}
	return b.String(), args
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
