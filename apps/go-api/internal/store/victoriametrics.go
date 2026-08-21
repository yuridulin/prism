package store

import (
	"bytes"
	"context"
	"encoding/csv"
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
		// Canonical ILP for all APIs: empty measurement, quality is a label,
		// field name prism_sample is the metric. Query match[] is prism_sample{tag_id}.
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
	at = at.UTC()
	seed, _, err := s.scanExport(ctx, tagIDs, at.Add(-vmLookback), at, at, at, false)
	return seed, err
}

func (s *VictoriaMetrics) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	from, to = from.UTC(), to.UTC()
	seed, mid, err := s.scanExport(ctx, tagIDs, from.Add(-vmLookback), to, from, to, true)
	if err != nil {
		return nil, err
	}
	return append(seed, mid...), nil
}

// scanExport pulls raw samples in one /api/v1/export/csv call.
// Archive max gap is 1h (hourly tags); 2h lookback is enough for locf at 364d ago.
func (s *VictoriaMetrics) scanExport(ctx context.Context, tagIDs []uint32, start, end, from, to time.Time, withMid bool) ([]model.Sample, []model.Sample, error) {
	if len(tagIDs) == 0 {
		return nil, nil, nil
	}
	params := url.Values{}
	params.Set("match[]", fmt.Sprintf(`prism_sample{tag_id=~"%s"}`, vmTagRE(tagIDs)))
	params.Set("start", strconv.FormatInt(start.Unix(), 10))
	params.Set("end", strconv.FormatInt(end.Unix(), 10))
	params.Set("format", "tag_id,quality,__value__,__timestamp__:unix_ms")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/api/v1/export/csv?"+params.Encode(), nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer closeHTTP(resp)
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, nil, fmt.Errorf("vm export status %d: %s", resp.StatusCode, body)
	}

	best := make(map[uint32]model.Sample, len(tagIDs))
	var mid []model.Sample
	cr := csv.NewReader(resp.Body)
	cr.ReuseRecord = true
	cr.FieldsPerRecord = -1
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if len(row) < 4 {
			continue
		}
		if row[0] == "tag_id" {
			continue
		}
		id64, err := strconv.ParseUint(row[0], 10, 32)
		if err != nil {
			continue
		}
		id := uint32(id64)
		q, _ := strconv.Atoi(row[1])
		val, _ := strconv.ParseFloat(row[2], 64)
		ms, _ := strconv.ParseInt(row[3], 10, 64)
		t := time.UnixMilli(ms).UTC()
		if !t.After(from) {
			if prev, ok := best[id]; !ok || t.After(prev.TS) {
				best[id] = model.Sample{TS: t, TagID: id, Value: val, Quality: uint16(q), Carried: withMid}
			}
			continue
		}
		if withMid && !t.After(to) {
			mid = append(mid, model.Sample{TS: t, TagID: id, Value: val, Quality: uint16(q)})
		}
	}

	seed := make([]model.Sample, 0, len(tagIDs))
	for _, id := range tagIDs {
		if sample, ok := best[id]; ok {
			seed = append(seed, sample)
		}
	}
	return seed, mid, nil
}

func (s *VictoriaMetrics) UpsertTags(_ context.Context, tags []model.Tag) error {
	s.tags.upsert(tags)
	return nil
}

func (s *VictoriaMetrics) ListTags(_ context.Context) ([]model.Tag, error) {
	return s.tags.list(), nil
}

const vmLookback = 2 * time.Hour

func vmTagRE(ids []uint32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return strings.Join(parts, "|")
}
