package store

import (
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

type QuestDB struct {
	base   string
	client *http.Client
}

type qdbExec struct {
	Query   string     `json:"query"`
	Columns []qdbCol   `json:"columns"`
	Dataset [][]any    `json:"dataset"`
	Error   string     `json:"error"`
}

type qdbCol struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func NewQuestDB(httpURL, _ string) (*QuestDB, error) {
	return &QuestDB{
		base:   strings.TrimRight(httpURL, "/"),
		client: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (s *QuestDB) Name() string { return "questdb" }

func (s *QuestDB) Close() error { return nil }

func (s *QuestDB) Ping(ctx context.Context) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.exec(ctx, "SELECT 1")
	return err
}

func (s *QuestDB) ensure(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS samples (ts TIMESTAMP, tag_id INT, value FLOAT, quality SHORT) timestamp(ts) PARTITION BY DAY WAL`,
		`CREATE TABLE IF NOT EXISTS tags (id INT, name SYMBOL, unit SYMBOL)`,
	}
	for _, q := range stmts {
		if _, err := s.exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (s *QuestDB) Write(ctx context.Context, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	var b strings.Builder
	for _, p := range samples {
		fmt.Fprintf(&b, "samples tag_id=%di,value=%g,quality=%di %d\n",
			p.TagID, p.Value, p.Quality, p.TS.UTC().UnixNano())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/write?precision=n", strings.NewReader(b.String()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("questdb write %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (s *QuestDB) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	q := fmt.Sprintf(
		`SELECT ts, tag_id, value, quality FROM samples WHERE tag_id IN (%s) AND ts <= '%s' LATEST ON ts PARTITION BY tag_id`,
		joinIDs(tagIDs), qdbTime(at),
	)
	data, err := s.exec(ctx, q)
	if err != nil {
		return nil, err
	}
	return parseQDBSamples(data, false)
}

func (s *QuestDB) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	ids := joinIDs(tagIDs)
	q := fmt.Sprintf(`
		SELECT ts, tag_id, value, quality, carried FROM (
			SELECT ts, tag_id, value, quality, true AS carried
			FROM samples
			WHERE tag_id IN (%s) AND ts <= '%s'
			LATEST ON ts PARTITION BY tag_id
			UNION ALL
			SELECT ts, tag_id, value, quality, false
			FROM samples
			WHERE tag_id IN (%s) AND ts > '%s' AND ts <= '%s'
		)
		ORDER BY tag_id, ts`, ids, qdbTime(from), ids, qdbTime(from), qdbTime(to))
	data, err := s.exec(ctx, q)
	if err != nil {
		return nil, err
	}
	return parseQDBSamples(data, true)
}

func (s *QuestDB) UpsertTags(ctx context.Context, tags []model.Tag) error {
	for _, t := range tags {
		name := strings.ReplaceAll(t.Name, "'", "''")
		unit := strings.ReplaceAll(t.Unit, "'", "''")
		q := fmt.Sprintf("INSERT INTO tags (id, name, unit) VALUES (%d, '%s', '%s')", t.ID, name, unit)
		if _, err := s.exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (s *QuestDB) ListTags(ctx context.Context) ([]model.Tag, error) {
	data, err := s.exec(ctx, "SELECT id, name, unit FROM tags ORDER BY id")
	if err != nil {
		return nil, err
	}
	var out []model.Tag
	for _, row := range data.Dataset {
		if len(row) < 3 {
			continue
		}
		out = append(out, model.Tag{
			ID:   uint32(asFloat(row[0])),
			Name: fmt.Sprint(row[1]),
			Unit: fmt.Sprint(row[2]),
		})
	}
	return out, nil
}

func (s *QuestDB) exec(ctx context.Context, query string) (*qdbExec, error) {
	u := s.base + "/exec?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out qdbExec
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, fmt.Errorf("questdb: %s", out.Error)
	}
	return &out, nil
}

func parseQDBSamples(data *qdbExec, hasCarried bool) ([]model.Sample, error) {
	var out []model.Sample
	for _, row := range data.Dataset {
		if len(row) < 4 {
			continue
		}
		ts, err := parseQDBTS(row[0])
		if err != nil {
			return nil, err
		}
		s := model.Sample{
			TS:      ts,
			TagID:   uint32(asFloat(row[1])),
			Value:   asFloat(row[2]),
			Quality: uint16(asFloat(row[3])),
		}
		if hasCarried && len(row) > 4 {
			s.Carried = asBool(row[4])
		}
		out = append(out, s)
	}
	return out, nil
}

func parseQDBTS(v any) (time.Time, error) {
	switch t := v.(type) {
	case string:
		if ts, err := time.Parse("2006-01-02T15:04:05.000000Z", t); err == nil {
			return ts, nil
		}
		return time.Parse(time.RFC3339Nano, t)
	case float64:
		return time.UnixMilli(int64(t)).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("questdb ts %T", v)
	}
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		return 0
	}
}

func asBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case float64:
		return t != 0
	case string:
		return t == "true" || t == "t" || t == "1"
	default:
		return false
	}
}

func joinIDs(ids []uint32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return strings.Join(parts, ",")
}

func qdbTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}
