package model

import (
	"time"
)

const (
	Contract     = "v1.2"
	QualityGood  = uint16(192)
	QualityUnc   = uint16(64)
	QualityBad   = uint16(0)
)

var Ops = []string{"write", "locf", "range", "tags"}

// Sample is the store-layer point. HTTP maps date/id onto these fields.
type Sample struct {
	TS      time.Time `json:"date"`
	TagID   uint32    `json:"id"`
	Value   float64   `json:"value"`
	Quality uint16    `json:"quality"`
	Carried bool      `json:"-"`
}

type WriteItem struct {
	Date    time.Time `json:"date"`
	TS      time.Time `json:"ts"`
	ID      uint32    `json:"id"`
	TagID   uint32    `json:"tag_id"`
	Value   float64   `json:"value"`
	Quality *uint16   `json:"quality"`
}

func (w WriteItem) Normalize(now time.Time) Sample {
	ts := w.Date
	if ts.IsZero() {
		ts = w.TS
	}
	if ts.IsZero() {
		ts = now
	}
	id := w.ID
	if id == 0 {
		id = w.TagID
	}
	q := QualityGood
	if w.Quality != nil {
		q = *w.Quality
	}
	return Sample{TS: ts.UTC(), TagID: id, Value: w.Value, Quality: q}
}

type SamplesWrap struct {
	Samples []WriteItem `json:"samples"`
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

type ValuesRequest struct {
	RequestKey string    `json:"requestKey"`
	TagsID     []uint32  `json:"tagsId"`
	Exact      time.Time `json:"exact"`
	Old        time.Time `json:"old"`
	Young      time.Time `json:"young"`
}

func (r ValuesRequest) Mode() string {
	if !r.Old.IsZero() && !r.Young.IsZero() {
		return "range"
	}
	return "locf"
}

func (r ValuesRequest) At() time.Time {
	if !r.Exact.IsZero() {
		return r.Exact.UTC()
	}
	return time.Now().UTC()
}

type ValueRecord struct {
	Date    time.Time `json:"date"`
	Value   float64   `json:"value"`
	Quality uint16    `json:"quality"`
}

type ValuesTag struct {
	ID     uint32        `json:"id"`
	Values []ValueRecord `json:"values"`
}

type ValuesResponse struct {
	RequestKey string      `json:"requestKey,omitempty"`
	Tags       []ValuesTag `json:"tags"`
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
