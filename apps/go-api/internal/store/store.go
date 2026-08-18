package store

import (
	"context"
	"errors"
	"fmt"

	"prism/go-api/internal/config"
	"prism/go-api/internal/model"
)

var ErrNotFound = errors.New("not found")

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
	switch cfg.Storage {
	case "timescaledb":
		return NewTimescale(cfg.PostgresDSN)
	case "clickhouse":
		return NewClickHouse(cfg.ClickHouseDSN)
	case "influxdb":
		return NewInflux(cfg)
	case "victoriametrics":
		return NewVictoriaMetrics(cfg.VMURL)
	default:
		return nil, fmt.Errorf("unsupported storage %q", cfg.Storage)
	}
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
