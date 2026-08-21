package store

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"prism/go-api/internal/model"
)

const ilpPoolSize = 8

type QuestDB struct {
	base    string
	ilpAddr string
	client  *http.Client
	conns   chan *ilpConn
}

type ilpConn struct {
	c net.Conn
	w *bufio.Writer
}

type qdbExec struct {
	Query   string   `json:"query"`
	Columns []qdbCol `json:"columns"`
	Dataset [][]any  `json:"dataset"`
	Error   string   `json:"error"`
}

type qdbCol struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func NewQuestDB(httpURL, ilpAddr string) (*QuestDB, error) {
	if ilpAddr == "" {
		ilpAddr = "questdb:9009"
	}
	return &QuestDB{
		base:    strings.TrimRight(httpURL, "/"),
		ilpAddr: ilpAddr,
		client:  newWriteHTTPClient(30 * time.Second),
		conns:   make(chan *ilpConn, ilpPoolSize),
	}, nil
}

func (s *QuestDB) Name() string { return "questdb" }

func (s *QuestDB) Close() error {
	for {
		select {
		case c := <-s.conns:
			c.close()
		default:
			return nil
		}
	}
}

func (s *QuestDB) Ping(ctx context.Context) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.exec(ctx, "SELECT 1")
	return err
}

func (s *QuestDB) ensure(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS samples (ts TIMESTAMP, tag_id SYMBOL CAPACITY 256 CACHE INDEX, value FLOAT, quality SHORT) timestamp(ts) PARTITION BY DAY WAL`,
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
	buf := getBuf()
	defer putBuf(buf)
	for i := range samples {
		p := &samples[i]
		buf.WriteString("samples tag_id=")
		buf.WriteString(strconv.FormatUint(uint64(p.TagID), 10))
		buf.WriteString("i,value=")
		buf.WriteString(strconv.FormatFloat(p.Value, 'g', -1, 64))
		buf.WriteString(",quality=")
		buf.WriteString(strconv.FormatUint(uint64(p.Quality), 10))
		buf.WriteString("i ")
		buf.WriteString(strconv.FormatInt(p.TS.UTC().UnixNano(), 10))
		buf.WriteByte('\n')
	}
	return s.sendILP(ctx, buf.Bytes())
}

func (s *QuestDB) sendILP(ctx context.Context, payload []byte) error {
	c, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	if err := writeILP(ctx, c, payload); err != nil {
		c.close()
		c2, err2 := s.dial(ctx)
		if err2 != nil {
			return err
		}
		if err := writeILP(ctx, c2, payload); err != nil {
			c2.close()
			return err
		}
		s.release(c2)
		return nil
	}
	s.release(c)
	return nil
}

func (s *QuestDB) acquire(ctx context.Context) (*ilpConn, error) {
	select {
	case c := <-s.conns:
		return c, nil
	default:
		return s.dial(ctx)
	}
}

func (s *QuestDB) release(c *ilpConn) {
	select {
	case s.conns <- c:
	default:
		c.close()
	}
}

func (s *QuestDB) dial(ctx context.Context) (*ilpConn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", s.ilpAddr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
	}
	return &ilpConn{c: c, w: bufio.NewWriterSize(c, 64<<10)}, nil
}

func writeILP(ctx context.Context, c *ilpConn, payload []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.c.SetWriteDeadline(deadline)
	} else {
		_ = c.c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	}
	if _, err := c.w.Write(payload); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *ilpConn) close() {
	if c != nil && c.c != nil {
		_ = c.c.Close()
	}
}

func (s *QuestDB) Locf(ctx context.Context, tagIDs []uint32, at time.Time) ([]model.Sample, error) {
	q := fmt.Sprintf(
		`SELECT ts, tag_id, value, quality FROM samples WHERE tag_id IN (%s) AND ts <= '%s' LATEST ON ts PARTITION BY tag_id`,
		joinSymbolIDs(tagIDs), qdbTime(at),
	)
	data, err := s.exec(ctx, q)
	if err != nil {
		return nil, err
	}
	return parseQDBSamples(data, false)
}

func (s *QuestDB) Range(ctx context.Context, tagIDs []uint32, from, to time.Time) ([]model.Sample, error) {
	ids := joinSymbolIDs(tagIDs)
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
		)`, ids, qdbTime(from), ids, qdbTime(from), qdbTime(to))
	return s.expSamples(ctx, q)
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
	defer closeHTTP(resp)
	var out qdbExec
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, fmt.Errorf("questdb: %s", out.Error)
	}
	return &out, nil
}

func (s *QuestDB) expSamples(ctx context.Context, query string) ([]model.Sample, error) {
	u := s.base + "/exp?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer closeHTTP(resp)
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("questdb exp %d: %s", resp.StatusCode, body)
	}
	return parseQDBCSV(resp.Body)
}

func parseQDBCSV(r io.Reader) ([]model.Sample, error) {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	cr.LazyQuotes = true
	header, err := cr.Read()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, name := range header {
		idx[strings.ToLower(strings.TrimSpace(name))] = i
	}
	tsI, okTS := idx["ts"]
	tagI, okTag := idx["tag_id"]
	valI, okVal := idx["value"]
	qI, okQ := idx["quality"]
	cI, hasCarried := idx["carried"]
	if !okTS || !okTag || !okVal || !okQ {
		return nil, fmt.Errorf("questdb csv columns %v", header)
	}
	out := make([]model.Sample, 0, 4096)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if tsI >= len(row) || tagI >= len(row) || valI >= len(row) || qI >= len(row) {
			continue
		}
		ts, err := parseQDBTS(row[tsI])
		if err != nil {
			return nil, err
		}
		tag, _ := strconv.ParseUint(row[tagI], 10, 32)
		val, _ := strconv.ParseFloat(row[valI], 64)
		q, _ := strconv.ParseUint(row[qI], 10, 16)
		s := model.Sample{TS: ts, TagID: uint32(tag), Value: val, Quality: uint16(q)}
		if hasCarried && cI < len(row) {
			s.Carried = asBool(row[cI])
		}
		out = append(out, s)
	}
	return out, nil
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

func joinSymbolIDs(ids []uint32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = "'" + strconv.FormatUint(uint64(id), 10) + "'"
	}
	return strings.Join(parts, ",")
}

func qdbTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}
