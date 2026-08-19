package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"prism/go-api/internal/config"
	"prism/go-api/internal/model"
)

type Influx struct {
	client   influxdb2.Client
	http     *http.Client
	writeURL string
	token    string
	org      string
	bucket   string
	tags     catalogMem
}

func NewInflux(cfg config.Config) (*Influx, error) {
	client := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	writeURL := strings.TrimRight(cfg.InfluxURL, "/") + "/api/v2/write?" + url.Values{
		"org":       {cfg.InfluxOrg},
		"bucket":    {cfg.InfluxBucket},
		"precision": {"ns"},
	}.Encode()
	return &Influx{
		client:   client,
		http:     newWriteHTTPClient(30 * time.Second),
		writeURL: writeURL,
		token:    cfg.InfluxToken,
		org:      cfg.InfluxOrg,
		bucket:   cfg.InfluxBucket,
	}, nil
}

func (s *Influx) Name() string { return "influxdb" }

func (s *Influx) Close() error {
	s.client.Close()
	s.http.CloseIdleConnections()
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

func (s *Influx) Write(ctx context.Context, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	buf := getBuf()
	defer putBuf(buf)
	for i := range samples {
		appendInfluxLine(buf, &samples[i])
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.writeURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+s.token)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer closeHTTP(resp)
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("influx write %d: %s", resp.StatusCode, body)
	}
	return nil
}

func appendInfluxLine(buf *bytes.Buffer, p *model.Sample) {
	buf.WriteString("samples,tag_id=")
	buf.WriteString(strconv.FormatUint(uint64(p.TagID), 10))
	buf.WriteString(" value=")
	buf.WriteString(strconv.FormatFloat(p.Value, 'g', -1, 64))
	buf.WriteString(",quality=")
	buf.WriteString(strconv.FormatUint(uint64(p.Quality), 10))
	buf.WriteString("i ")
	buf.WriteString(strconv.FormatInt(p.TS.UTC().UnixNano(), 10))
	buf.WriteByte('\n')
}

func (s *Influx) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	return s.queryLast(ctx, tagIDs, time.Time{}, at.UTC(), false)
}

func (s *Influx) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	seed, err := s.queryLast(ctx, tagIDs, time.Time{}, from.UTC(), true)
	if err != nil {
		return nil, err
	}
	mid, err := s.queryWindow(ctx, tagIDs, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	return append(seed, mid...), nil
}

func (s *Influx) queryLast(ctx context.Context, tagIDs []uint32, start, stop time.Time, carried bool) ([]model.Sample, error) {
	startRaw := "-30d"
	if !start.IsZero() {
		startRaw = start.Format(time.RFC3339Nano)
	}
	flux := fmt.Sprintf(`
from(bucket: %q)
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => %s)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["tag_id"])
  |> last()
`, s.bucket, startRaw, stop.Format(time.RFC3339Nano), influxTagFilter(tagIDs))
	return s.collect(ctx, flux, carried)
}

func (s *Influx) queryWindow(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	flux := fmt.Sprintf(`
from(bucket: %q)
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => %s)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
`, s.bucket, from.Add(time.Nanosecond).Format(time.RFC3339Nano), to.Add(time.Nanosecond).Format(time.RFC3339Nano), influxTagFilter(tagIDs))
	return s.collect(ctx, flux, false)
}

func (s *Influx) collect(ctx context.Context, flux string, carried bool) ([]model.Sample, error) {
	result, err := s.client.QueryAPI(s.org).Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	var out []model.Sample
	for result.Next() {
		rec := result.Record()
		id, _ := strconv.ParseUint(fmt.Sprint(rec.ValueByKey("tag_id")), 10, 32)
		val, _ := rec.ValueByKey("value").(float64)
		q := uint16(0)
		switch raw := rec.ValueByKey("quality").(type) {
		case int64:
			q = uint16(raw)
		case float64:
			q = uint16(raw)
		}
		out = append(out, model.Sample{TS: rec.Time(), TagID: uint32(id), Value: val, Quality: q, Carried: carried})
	}
	return out, result.Err()
}

func (s *Influx) UpsertTags(_ context.Context, tags []model.Tag) error {
	s.tags.upsert(tags)
	return nil
}

func (s *Influx) ListTags(_ context.Context) ([]model.Tag, error) {
	return s.tags.list(), nil
}

func influxTagFilter(ids []uint32) string {
	if len(ids) == 0 {
		return "true"
	}
	expr := ""
	for i, id := range ids {
		if i > 0 {
			expr += " or "
		}
		expr += fmt.Sprintf(`r.tag_id == %q`, strconv.FormatUint(uint64(id), 10))
	}
	return expr
}

type catalogMem struct {
	mu   sync.Mutex
	data map[uint32]model.Tag
}

func (c *catalogMem) upsert(tags []model.Tag) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = map[uint32]model.Tag{}
	}
	for _, t := range tags {
		c.data[t.ID] = t
	}
}

func (c *catalogMem) list() []model.Tag {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Tag, 0, len(c.data))
	for _, t := range c.data {
		out = append(out, t)
	}
	return out
}
