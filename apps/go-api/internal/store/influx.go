package store

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
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
	base     string
	writeURL string
	token    string
	org      string
	bucket   string
	tags     catalogMem
}

func NewInflux(cfg config.Config) (*Influx, error) {
	base := strings.TrimRight(cfg.InfluxURL, "/")
	client := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	writeURL := base + "/api/v2/write?" + url.Values{
		"org":       {cfg.InfluxOrg},
		"bucket":    {cfg.InfluxBucket},
		"precision": {"ns"},
	}.Encode()
	return &Influx{
		client:   client,
		http:     newWriteHTTPClient(30 * time.Second),
		base:     base,
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
	return s.ensureDBRP(ctx)
}

type influxDBRPList struct {
	Content []json.RawMessage `json:"content"`
}

type influxBucketList struct {
	Buckets []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"buckets"`
}

func (s *Influx) ensureDBRP(ctx context.Context) error {
	q := url.Values{"org": {s.org}, "db": {s.bucket}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/api/v2/dbrps?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+s.token)
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	closeHTTP(resp)
	if resp.StatusCode < 300 {
		var listed influxDBRPList
		if err := json.Unmarshal(body, &listed); err == nil && len(listed.Content) > 0 {
			return nil
		}
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/api/v2/buckets?"+url.Values{"org": {s.org}, "name": {s.bucket}}.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+s.token)
	resp, err = s.http.Do(req)
	if err != nil {
		return err
	}
	body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	closeHTTP(resp)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("influx buckets %d: %s", resp.StatusCode, body)
	}
	var buckets influxBucketList
	if err := json.Unmarshal(body, &buckets); err != nil || len(buckets.Buckets) == 0 {
		return fmt.Errorf("influx bucket %q not found", s.bucket)
	}
	payload, _ := json.Marshal(map[string]any{
		"org":              s.org,
		"bucketID":         buckets.Buckets[0].ID,
		"database":         s.bucket,
		"retention_policy": "autogen",
		"default":          true,
	})
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/api/v2/dbrps?org="+url.QueryEscape(s.org), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+s.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err = s.http.Do(req)
	if err != nil {
		return err
	}
	body, _ = io.ReadAll(io.LimitReader(resp.Body, 2048))
	closeHTTP(resp)
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("influx dbrp %d: %s", resp.StatusCode, body)
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
	return s.queryLast(ctx, tagIDs, at.UTC(), false)
}

func (s *Influx) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	seed, err := s.queryLast(ctx, tagIDs, from.UTC(), true)
	if err != nil {
		return nil, err
	}
	mid, err := s.queryWindow(ctx, tagIDs, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	return append(seed, mid...), nil
}

func (s *Influx) queryLast(ctx context.Context, tagIDs []uint32, stop time.Time, carried bool) ([]model.Sample, error) {
	q := fmt.Sprintf(
		`SELECT last("value") AS "value", last("quality") AS "quality" FROM "samples" WHERE time <= %s AND %s GROUP BY "tag_id"`,
		influxQLTime(stop), influxQLTagRE(tagIDs),
	)
	return s.queryInfluxQL(ctx, q, carried)
}

func (s *Influx) queryWindow(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	q := fmt.Sprintf(
		`SELECT "value", "quality" FROM "samples" WHERE time > %s AND time <= %s AND %s`,
		influxQLTime(from), influxQLTime(to), influxQLTagRE(tagIDs),
	)
	return s.queryInfluxQL(ctx, q, false)
}

type influxQLResp struct {
	Results []struct {
		Error  string `json:"error"`
		Series []struct {
			Tags    map[string]string `json:"tags"`
			Columns []string          `json:"columns"`
			Values  [][]any           `json:"values"`
		} `json:"series"`
	} `json:"results"`
}

func (s *Influx) queryInfluxQL(ctx context.Context, q string, carried bool) ([]model.Sample, error) {
	form := url.Values{
		"org":    {s.org},
		"bucket": {s.bucket},
		"db":     {s.bucket},
		"epoch":  {"ms"},
		"q":      {q},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/query", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+s.token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/csv")
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer closeHTTP(resp)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("influx query %d: %s", resp.StatusCode, body)
	}
	if len(body) == 0 || body[0] != '{' {
		return parseInfluxCSV(body, carried)
	}
	var parsed influxQLResp
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	out := make([]model.Sample, 0, 64)
	for _, result := range parsed.Results {
		if result.Error != "" {
			return nil, fmt.Errorf("influxql: %s", result.Error)
		}
		for _, series := range result.Series {
			id, _ := strconv.ParseUint(series.Tags["tag_id"], 10, 32)
			col := map[string]int{}
			for i, name := range series.Columns {
				col[name] = i
			}
			ti, okT := col["time"]
			vi, okV := col["value"]
			qi, hasQ := col["quality"]
			if !okT || !okV {
				continue
			}
			for _, row := range series.Values {
				if ti >= len(row) || vi >= len(row) {
					continue
				}
				ts := influxQLTS(row[ti])
				val := asFloat(row[vi])
				q := uint16(0)
				if hasQ && qi < len(row) {
					q = uint16(asFloat(row[qi]))
				}
				out = append(out, model.Sample{TS: ts, TagID: uint32(id), Value: val, Quality: q, Carried: carried})
			}
		}
	}
	return out, nil
}

func influxQLTime(t time.Time) string {
	return "'" + t.UTC().Format(time.RFC3339Nano) + "'"
}

func influxQLTagRE(ids []uint32) string {
	if len(ids) == 0 {
		return "true"
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return `tag_id =~ /^(` + strings.Join(parts, "|") + `)$/`
}

func influxQLTS(v any) time.Time {
	switch t := v.(type) {
	case float64:
		return time.UnixMilli(int64(t)).UTC()
	case json.Number:
		n, _ := t.Int64()
		return time.UnixMilli(n).UTC()
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return ts.UTC()
		}
		if n, err := strconv.ParseInt(t, 10, 64); err == nil {
			return time.UnixMilli(n).UTC()
		}
	}
	return time.Time{}
}

func parseInfluxCSV(body []byte, carried bool) ([]model.Sample, error) {
	cr := csv.NewReader(bytes.NewReader(body))
	cr.ReuseRecord = true
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1
	idx := map[string]int{}
	header := false
	out := make([]model.Sample, 0, 1024)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(row) == 0 || strings.HasPrefix(row[0], "#") {
			continue
		}
		if !header {
			for i, name := range row {
				idx[strings.ToLower(strings.TrimSpace(name))] = i
			}
			if _, ok := idx["time"]; !ok {
				continue
			}
			header = true
			continue
		}
		ti, okT := idx["time"]
		vi, okV := idx["value"]
		if !okT || !okV || ti >= len(row) || vi >= len(row) {
			continue
		}
		id := uint32(0)
		if i, ok := idx["tag_id"]; ok && i < len(row) {
			n, _ := strconv.ParseUint(row[i], 10, 32)
			id = uint32(n)
		} else if i, ok := idx["tags"]; ok && i < len(row) {
			id = tagIDFromInfluxTags(row[i])
		}
		q := uint16(0)
		if i, ok := idx["quality"]; ok && i < len(row) {
			f, _ := strconv.ParseFloat(row[i], 64)
			q = uint16(f)
		}
		val, _ := strconv.ParseFloat(row[vi], 64)
		out = append(out, model.Sample{
			TS: influxQLTS(row[ti]), TagID: id, Value: val, Quality: q, Carried: carried,
		})
	}
	return out, nil
}

func tagIDFromInfluxTags(s string) uint32 {
	s = strings.Trim(s, `"`)
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && k == "tag_id" {
			n, _ := strconv.ParseUint(v, 10, 32)
			return uint32(n)
		}
	}
	return 0
}

func (s *Influx) UpsertTags(_ context.Context, tags []model.Tag) error {
	s.tags.upsert(tags)
	return nil
}

func (s *Influx) ListTags(_ context.Context) ([]model.Tag, error) {
	return s.tags.list(), nil
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
