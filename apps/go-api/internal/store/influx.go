package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"prism/go-api/internal/config"
	"prism/go-api/internal/model"
)

type Influx struct {
	client influxdb2.Client
	org    string
	bucket string
}

func NewInflux(cfg config.Config) (*Influx, error) {
	client := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	return &Influx{client: client, org: cfg.InfluxOrg, bucket: cfg.InfluxBucket}, nil
}

func (s *Influx) Name() string { return "influxdb" }

func (s *Influx) Close() error {
	s.client.Close()
	return nil
}

func (s *Influx) Ping(ctx context.Context) error {
	ok, err := s.client.Ping(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("influxdb ping failed")
	}
	return nil
}

func (s *Influx) Write(ctx context.Context, points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	api := s.client.WriteAPIBlocking(s.org, s.bucket)
	batch := make([]*write.Point, 0, len(points))
	for _, p := range points {
		tags := model.NormalizeLabels(p.Labels)
		tags["metric"] = p.Metric
		batch = append(batch, influxdb2.NewPoint("prism", tags, map[string]any{"value": p.Value}, p.TS.UTC()))
	}
	return api.WritePoint(ctx, batch...)
}

func (s *Influx) Query(ctx context.Context, q model.Query) (*model.QueryResult, error) {
	flux := fmt.Sprintf(`
from(bucket: %q)
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "prism" and r.metric == %q and r._field == "value")
  %s
  |> aggregateWindow(every: %s, fn: %s, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
`, s.bucket, q.From.UTC().Format(time.RFC3339Nano), q.To.UTC().Format(time.RFC3339Nano),
		q.Metric, fluxLabelFilters(q.Labels), fluxDuration(q.Step), fluxAgg(q.Agg))

	result, err := s.client.QueryAPI(s.org).Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	out := &model.QueryResult{Metric: q.Metric, Agg: q.Agg, Step: q.Step.String(), Points: []model.Sample{}}
	for result.Next() {
		rec := result.Record()
		val, ok := rec.Value().(float64)
		if !ok {
			if n, ok := rec.Value().(int64); ok {
				val = float64(n)
			}
		}
		out.Points = append(out.Points, model.Sample{TS: rec.Time(), Value: val})
	}
	return out, result.Err()
}

func (s *Influx) Latest(ctx context.Context, metric string, labels map[string]string) (*model.Point, error) {
	flux := fmt.Sprintf(`
from(bucket: %q)
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "prism" and r.metric == %q and r._field == "value")
  %s
  |> last()
`, s.bucket, metric, fluxLabelFilters(labels))

	result, err := s.client.QueryAPI(s.org).Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	if result.Next() {
		rec := result.Record()
		val, _ := rec.Value().(float64)
		outLabels := map[string]string{}
		for k, v := range rec.Values() {
			if sv, ok := v.(string); ok && k != "_measurement" && k != "_field" && k != "metric" && k != "result" && k != "table" {
				outLabels[k] = sv
			}
		}
		return &model.Point{TS: rec.Time(), Metric: metric, Value: val, Labels: outLabels}, result.Err()
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNotFound
}

func fluxAgg(agg string) string {
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
		return "mean"
	}
}

func fluxDuration(d time.Duration) string {
	sec := int(d.Seconds())
	if sec < 1 {
		sec = 1
	}
	return fmt.Sprintf("%ds", sec)
}

func fluxLabelFilters(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var b string
	for k, v := range labels {
		b += fmt.Sprintf(`  |> filter(fn: (r) => r[%q] == %q)`+"\n", k, v)
	}
	return b
}
