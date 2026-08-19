package store

import (
	"context"
	"time"

	"prism/go-api/internal/metrics"
	"prism/go-api/internal/model"
)

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

func (s *Observed) Write(ctx context.Context, samples []model.Sample) error {
	start := time.Now()
	err := s.inner.Write(ctx, samples)
	metrics.ObserveStorage(s.Name(), "write", time.Since(start), err)
	return err
}

func (s *Observed) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	start := time.Now()
	res, err := s.inner.Locf(ctx, tagIDs, at)
	metrics.ObserveStorage(s.Name(), "locf", time.Since(start), err)
	return res, err
}

func (s *Observed) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	start := time.Now()
	res, err := s.inner.Range(ctx, tagIDs, from, to)
	metrics.ObserveStorage(s.Name(), "range", time.Since(start), err)
	return res, err
}

func (s *Observed) UpsertTags(ctx context.Context, tags []model.Tag) error {
	start := time.Now()
	err := s.inner.UpsertTags(ctx, tags)
	metrics.ObserveStorage(s.Name(), "tags", time.Since(start), err)
	return err
}

func (s *Observed) ListTags(ctx context.Context) ([]model.Tag, error) {
	start := time.Now()
	res, err := s.inner.ListTags(ctx)
	metrics.ObserveStorage(s.Name(), "tags", time.Since(start), err)
	return res, err
}
