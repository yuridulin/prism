package store

import (
	"context"
	"time"

	"prism/go-api/internal/metrics"
	"prism/go-api/internal/model"
)

// Observed wraps a Store and records storage-layer metrics.
// New adapters get telemetry automatically when constructed via New.
type Observed struct {
	inner Store
}

func Observe(inner Store) Store {
	return &Observed{inner: inner}
}

func (s *Observed) Name() string { return s.inner.Name() }

func (s *Observed) Close() error { return s.inner.Close() }

func (s *Observed) Ping(ctx context.Context) error {
	start := time.Now()
	err := s.inner.Ping(ctx)
	metrics.ObserveStorage(s.Name(), "ping", time.Since(start), err)
	return err
}

func (s *Observed) Write(ctx context.Context, points []model.Point) error {
	start := time.Now()
	err := s.inner.Write(ctx, points)
	metrics.ObserveStorage(s.Name(), "write", time.Since(start), err)
	return err
}

func (s *Observed) Query(ctx context.Context, q model.Query) (*model.QueryResult, error) {
	start := time.Now()
	res, err := s.inner.Query(ctx, q)
	metrics.ObserveStorage(s.Name(), "query", time.Since(start), err)
	return res, err
}

func (s *Observed) Latest(ctx context.Context, metric string, labels map[string]string) (*model.Point, error) {
	start := time.Now()
	res, err := s.inner.Latest(ctx, metric, labels)
	metrics.ObserveStorage(s.Name(), "latest", time.Since(start), err)
	return res, err
}
