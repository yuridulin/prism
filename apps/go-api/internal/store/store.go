package store

import (
	"context"
	"fmt"
	"time"

	"prism/go-api/internal/apperr"
	"prism/go-api/internal/config"
	"prism/go-api/internal/model"
)

var ErrNotFound = apperr.ErrNotFound

var Supported = []string{"timescaledb", "clickhouse", "questdb", "influxdb", "victoriametrics"}

type Store interface {
	Name() string
	Ping(ctx context.Context) error
	Write(ctx context.Context, samples []model.Sample) error
	Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error)
	Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error)
	UpsertTags(ctx context.Context, tags []model.Tag) error
	ListTags(ctx context.Context) ([]model.Tag, error)
	Close() error
}

func New(cfg config.Config) (Store, error) {
	var (
		inner Store
		err   error
	)
	switch cfg.Storage {
	case "timescaledb":
		inner, err = NewTimescale(cfg.PostgresDSN)
	case "clickhouse":
		inner, err = NewClickHouse(cfg.ClickHouseDSN)
	case "questdb":
		inner, err = NewQuestDB(cfg.QuestDBURL, cfg.QuestDBILP)
	case "influxdb":
		inner, err = NewInflux(cfg)
	case "victoriametrics":
		inner, err = NewVictoriaMetrics(cfg.VMURL)
	default:
		return nil, fmt.Errorf("unsupported storage %q", cfg.Storage)
	}
	if err != nil {
		return nil, err
	}
	return Observe(inner), nil
}
