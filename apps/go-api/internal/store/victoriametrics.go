package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"prism/go-api/internal/model"
)

type VictoriaMetrics struct {
	base     string
	writeURL string
	client   *http.Client
	tags     catalogMem
}

func NewVictoriaMetrics(base string) (*VictoriaMetrics, error) {
	base = strings.TrimRight(base, "/")
	return &VictoriaMetrics{
		base:     base,
		writeURL: base + "/write?precision=ns",
		client:   newWriteHTTPClient(15 * time.Second),
	}, nil
}

func (s *VictoriaMetrics) Name() string { return "victoriametrics" }

func (s *VictoriaMetrics) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

func (s *VictoriaMetrics) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer closeHTTP(resp)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("vm health status %d", resp.StatusCode)
	}
	return nil
}

func (s *VictoriaMetrics) Write(ctx context.Context, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	buf := getBuf()
	defer putBuf(buf)
	for i := range samples {
		p := &samples[i]
		// Empty measurement: VM uses the field name as the metric, so locf/range
		// still query prism_sample{tag_id,quality} like the old Prometheus import.
		buf.WriteString(",tag_id=")
		buf.WriteString(strconv.FormatUint(uint64(p.TagID), 10))
		buf.WriteString(",quality=")
		buf.WriteString(strconv.FormatUint(uint64(p.Quality), 10))
		buf.WriteString(" prism_sample=")
		buf.WriteString(strconv.FormatFloat(p.Value, 'g', -1, 64))
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatInt(p.TS.UTC().UnixNano(), 10))
		buf.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.writeURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer closeHTTP(resp)
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("vm write status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (s *VictoriaMetrics) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	var out []model.Sample
	for _, id := range tagIDs {
		params := url.Values{}
		params.Set("query", fmt.Sprintf(`last_over_time(prism_sample{tag_id="%d"}[30d])`, id))
		params.Set("time", strconv.FormatInt(at.UTC().Unix(), 10))
		var parsed vmInstantResponse
		if err := s.getJSON(ctx, "/api/v1/query?"+params.Encode(), &parsed); err != nil {
			return nil, err
		}
		if len(parsed.Data.Result) == 0 {
			continue
		}
		sample := parsed.Data.Result[0]
		ts, _ := sample.Value[0].(float64)
		raw, _ := sample.Value[1].(string)
		val, _ := strconv.ParseFloat(raw, 64)
		q := uint16(0)
		if v, ok := sample.Metric["quality"]; ok {
			n, _ := strconv.Atoi(v)
			q = uint16(n)
		}
		out = append(out, model.Sample{TS: time.Unix(int64(ts), 0).UTC(), TagID: id, Value: val, Quality: q})
	}
	return out, nil
}

func (s *VictoriaMetrics) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	seed, err := s.Locf(ctx, tagIDs, from)
	if err != nil {
		return nil, err
	}
	for i := range seed {
		seed[i].Carried = true
	}
	var mid []model.Sample
	for _, id := range tagIDs {
		params := url.Values{}
		params.Set("match[]", fmt.Sprintf(`prism_sample{tag_id="%d"}`, id))
		params.Set("start", strconv.FormatInt(from.UTC().Unix(), 10))
		params.Set("end", strconv.FormatInt(to.UTC().Unix(), 10))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/api/v1/export?"+params.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(resp.Body)
		for dec.More() {
			var row struct {
				Metric     map[string]string `json:"metric"`
				Timestamps []int64           `json:"timestamps"`
				Values     []float64         `json:"values"`
			}
			if err := dec.Decode(&row); err != nil {
				closeHTTP(resp)
				return nil, err
			}
			q := uint16(0)
			if v, ok := row.Metric["quality"]; ok {
				n, _ := strconv.Atoi(v)
				q = uint16(n)
			}
			for i, ts := range row.Timestamps {
				t := time.UnixMilli(ts).UTC()
				if !t.After(from) || t.After(to) {
					continue
				}
				val := 0.0
				if i < len(row.Values) {
					val = row.Values[i]
				}
				mid = append(mid, model.Sample{TS: t, TagID: id, Value: val, Quality: q})
			}
		}
		closeHTTP(resp)
	}
	return append(seed, mid...), nil
}

func (s *VictoriaMetrics) UpsertTags(_ context.Context, tags []model.Tag) error {
	s.tags.upsert(tags)
	return nil
}

func (s *VictoriaMetrics) ListTags(_ context.Context) ([]model.Tag, error) {
	return s.tags.list(), nil
}

func (s *VictoriaMetrics) getJSON(ctx context.Context, path string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer closeHTTP(resp)
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("vm query status %d: %s", resp.StatusCode, body)
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

type vmInstantResponse struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
}
