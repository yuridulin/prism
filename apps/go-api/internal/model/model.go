package model

import "time"

const Contract = "v1"

var Ops = []string{"write", "query", "latest"}

type Point struct {
	TS     time.Time         `json:"ts"`
	Metric string            `json:"metric"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

type WriteRequest struct {
	Points []Point `json:"points"`
}

type WriteResponse struct {
	Written int `json:"written"`
}

type QueryRequest struct {
	Metric string            `json:"metric"`
	From   time.Time         `json:"from"`
	To     time.Time         `json:"to"`
	Step   string            `json:"step"`
	Agg    string            `json:"agg"`
	Labels map[string]string `json:"labels"`
}

type LatestRequest struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
}

type Query struct {
	Metric  string
	From    time.Time
	To      time.Time
	Step    time.Duration
	StepRaw string
	Agg     string
	Labels  map[string]string
}

type Sample struct {
	TS    time.Time `json:"ts"`
	Value float64   `json:"value"`
}

type QueryResult struct {
	Metric string   `json:"metric"`
	Agg    string   `json:"agg"`
	Step   string   `json:"step"`
	Points []Sample `json:"points"`
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

func NormalizeLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}

func ValidAgg(agg string) bool {
	switch agg {
	case "avg", "min", "max", "sum", "count":
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

func NormalizeQuery(req QueryRequest) Query {
	agg := req.Agg
	if agg == "" {
		agg = "avg"
	}
	stepRaw := req.Step
	if stepRaw == "" {
		stepRaw = "1m"
	}
	return Query{
		Metric:  req.Metric,
		From:    req.From.UTC(),
		To:      req.To.UTC(),
		Step:    ParseStep(stepRaw),
		StepRaw: stepRaw,
		Agg:     agg,
		Labels:  NormalizeLabels(req.Labels),
	}
}
