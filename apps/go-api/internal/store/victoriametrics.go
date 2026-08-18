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
	base   string
	client *http.Client
}

func NewVictoriaMetrics(base string) (*VictoriaMetrics, error) {
	return &VictoriaMetrics{
		base:   strings.TrimRight(base, "/"),
		client: &http.Client{Timeout: 15 * time.Second},
	}, nil
}

func (s *VictoriaMetrics) Name() string { return "victoriametrics" }

func (s *VictoriaMetrics) Close() error { return nil }

func (s *VictoriaMetrics) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("vm health status %d", resp.StatusCode)
	}
	return nil
}

func (s *VictoriaMetrics) Write(ctx context.Context, points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, p := range points {
		buf.WriteString(`prism_metric{metric="`)
		buf.WriteString(escapeLabel(p.Metric))
		buf.WriteByte('"')
		for k, v := range model.NormalizeLabels(p.Labels) {
			buf.WriteByte(',')
			buf.WriteString(escapeLabel(k))
			buf.WriteString(`="`)
			buf.WriteString(escapeLabel(v))
			buf.WriteByte('"')
		}
		buf.WriteString("} ")
		buf.WriteString(strconv.FormatFloat(p.Value, 'f', -1, 64))
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatInt(p.TS.UTC().UnixMilli(), 10))
		buf.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/api/v1/import/prometheus", &buf)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vm write status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (s *VictoriaMetrics) Query(ctx context.Context, q model.Query) (*model.QueryResult, error) {
	promql := fmt.Sprintf(`%s(prism_metric{%s})`, vmAgg(q.Agg), promMatchers(q.Metric, q.Labels))
	params := url.Values{}
	params.Set("query", promql)
	params.Set("start", strconv.FormatInt(q.From.UTC().Unix(), 10))
	params.Set("end", strconv.FormatInt(q.To.UTC().Unix(), 10))
	params.Set("step", fmt.Sprintf("%ds", max(int(q.Step.Seconds()), 1)))

	var parsed vmRangeResponse
	if err := s.getJSON(ctx, "/api/v1/query_range?"+params.Encode(), &parsed); err != nil {
		return nil, err
	}
	out := &model.QueryResult{Metric: q.Metric, Agg: q.Agg, Step: q.Step.String(), Points: []model.Sample{}}
	if len(parsed.Data.Result) == 0 {
		return out, nil
	}
	for _, pair := range parsed.Data.Result[0].Values {
		ts, _ := pair[0].(float64)
		raw, _ := pair[1].(string)
		val, _ := strconv.ParseFloat(raw, 64)
		out.Points = append(out.Points, model.Sample{TS: time.Unix(int64(ts), 0).UTC(), Value: val})
	}
	return out, nil
}

func (s *VictoriaMetrics) Latest(ctx context.Context, metric string, labels map[string]string) (*model.Point, error) {
	params := url.Values{}
	params.Set("query", fmt.Sprintf(`prism_metric{%s}`, promMatchers(metric, labels)))
	var parsed vmInstantResponse
	if err := s.getJSON(ctx, "/api/v1/query?"+params.Encode(), &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Data.Result) == 0 {
		return nil, ErrNotFound
	}
	sample := parsed.Data.Result[0]
	ts, _ := sample.Value[0].(float64)
	raw, _ := sample.Value[1].(string)
	val, _ := strconv.ParseFloat(raw, 64)
	outLabels := map[string]string{}
	for k, v := range sample.Metric {
		if k != "__name__" && k != "metric" {
			outLabels[k] = v
		}
	}
	return &model.Point{TS: time.Unix(int64(ts), 0).UTC(), Metric: metric, Value: val, Labels: outLabels}, nil
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
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vm query status %d: %s", resp.StatusCode, body)
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func vmAgg(agg string) string {
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

func promMatchers(metric string, labels map[string]string) string {
	parts := []string{fmt.Sprintf(`metric="%s"`, escapeLabel(metric))}
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, escapeLabel(k), escapeLabel(v)))
	}
	return strings.Join(parts, ",")
}

func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", "")
	return s
}

type vmRangeResponse struct {
	Data struct {
		Result []struct {
			Values [][]any `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

type vmInstantResponse struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
}
