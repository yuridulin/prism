package model

import "time"

type Point struct {
	TS     time.Time         `json:"ts"`
	Metric string            `json:"metric"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

type WriteRequest struct {
	Points []Point `json:"points"`
}

type Query struct {
	Metric string
	From   time.Time
	To     time.Time
	Step   time.Duration
	Agg    string
	Labels map[string]string
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
