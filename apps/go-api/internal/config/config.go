package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	HTTPAddr      string
	Storage       string
	PostgresDSN   string
	ClickHouseDSN string
	QuestDBURL    string
	QuestDBILP    string
	InfluxURL     string
	InfluxToken   string
	InfluxOrg     string
	InfluxBucket  string
	VMURL         string
	NATSURL       string
	NATSSubject   string
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:      env("HTTP_ADDR", ":8081"),
		Storage:       strings.ToLower(env("PRISM_STORAGE", "questdb")),
		PostgresDSN:   env("POSTGRES_DSN", "postgres://prism:prism@timescaledb:5432/prism?sslmode=disable"),
		ClickHouseDSN: env("CLICKHOUSE_DSN", "clickhouse://prism:prism@clickhouse:9000/prism"),
		QuestDBURL:    env("QUESTDB_URL", "http://questdb:9000"),
		QuestDBILP:    env("QUESTDB_ILP", "questdb:9009"),
		InfluxURL:     env("INFLUX_URL", "http://influxdb:8086"),
		InfluxToken:   env("INFLUX_TOKEN", "prism-dev-token"),
		InfluxOrg:     env("INFLUX_ORG", "prism"),
		InfluxBucket:  env("INFLUX_BUCKET", "prism"),
		VMURL:         env("VM_URL", "http://victoriametrics:8428"),
		NATSURL:       env("NATS_URL", "nats://nats:4222"),
		NATSSubject:   env("NATS_SUBJECT", "prism.samples"),
	}
	switch cfg.Storage {
	case "timescaledb", "clickhouse", "questdb", "influxdb", "victoriametrics":
	default:
		return cfg, fmt.Errorf("unknown PRISM_STORAGE %q", cfg.Storage)
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
