package store

import (
	"context"
	"fmt"

	"prism/go-api/internal/apperr"
	"prism/go-api/internal/config"
	"prism/go-api/internal/model"
)

var ErrNotFound = apperr.ErrNotFound

var Supported = []string{"timescaledb", "clickhouse", "influxdb", "victoriametrics"}

type Store interface {
	Name() string
	Ping(ctx context.Context) error
	Write(ctx context.Context, points []model.Point) error
	Query(ctx context.Context, q model.Query) (*model.QueryResult, error)
	Latest(ctx context.Context, metric string, labels map[string]string) (*model.Point, error)
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

func aggSQL(agg string) string {
	switch agg {
	case "min":
		return "min"
	case "max":
		return "max"
	case "sum":
		return "sum"
	case "count":
		return "count"
	default:
		return "avg"
	}
}
