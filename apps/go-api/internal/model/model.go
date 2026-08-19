package model

import "time"

const (
	Contract     = "v1.1"
	QualityGood  = uint16(192)
	QualityUnc   = uint16(64)
	QualityBad   = uint16(0)
)

var Ops = []string{"write", "locf", "range", "sample", "twavg", "tags"}

type Sample struct {
	TS      time.Time `json:"ts"`
	TagID   uint32    `json:"tag_id"`
	Value   float64   `json:"value"`
	Quality uint16    `json:"quality"`
	Carried bool      `json:"carried,omitempty"`
}

type WriteSample struct {
	TS      time.Time `json:"ts"`
	TagID   uint32    `json:"tag_id"`
	Value   float64   `json:"value"`
	Quality *uint16   `json:"quality"`
}

func (w WriteSample) Normalize(now time.Time) Sample {
	ts := w.TS
	if ts.IsZero() {
		ts = now
	}
	q := QualityGood
	if w.Quality != nil {
		q = *w.Quality
	}
	return Sample{TS: ts.UTC(), TagID: w.TagID, Value: w.Value, Quality: q}
}

type WriteRequest struct {
	Samples []WriteSample `json:"samples"`
}

type WriteResponse struct {
	Written int `json:"written"`
}

type Tag struct {
	ID   uint32 `json:"id"`
	Name string `json:"name"`
	Unit string `json:"unit,omitempty"`
}

type TagList struct {
	Tags []Tag `json:"tags"`
}

type TagWriteRequest struct {
	Tags []Tag `json:"tags"`
}

type TagWriteResponse struct {
	Upserted int `json:"upserted"`
}

type ReadRequest struct {
	Mode   string    `json:"mode"`
	TagIDs []uint32  `json:"tag_ids"`
	At     time.Time `json:"at"`
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Step   string    `json:"step"`
}

type Series struct {
	TagID   uint32   `json:"tag_id"`
	Value   *float64 `json:"value,omitempty"`
	Samples []Sample `json:"samples"`
}

type ReadResult struct {
	Mode   string     `json:"mode"`
	At     *time.Time `json:"at,omitempty"`
	From   *time.Time `json:"from,omitempty"`
	To     *time.Time `json:"to,omitempty"`
	Step   string     `json:"step,omitempty"`
	Series []Series   `json:"series"`
}

type Meta struct {
	Backend  string   `json:"backend"`
	Storage  string   `json:"storage"`
	Storages []string `json:"storages"`
	Contract string   `json:"contract"`
	Ops      []string `json:"ops"`
}

type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func ValidMode(mode string) bool {
	switch mode {
	case "locf", "range", "sample", "twavg":
		return true
	default:
		return false
	}
}

func ParseStep(raw string) time.Duration {
	if raw == "" {
		return time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return time.Minute
	}
	return d
}

func TimePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}
