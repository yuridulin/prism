package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	IngestPoints = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_ingest_points_total",
		Help: "Written time-series points",
	}, []string{"backend", "storage"})

	IngestErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_ingest_errors_total",
		Help: "Failed ingest batches",
	}, []string{"backend", "storage"})

	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "prism_query_duration_seconds",
		Help:    "Query latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"backend", "storage"})

	QueryErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_query_errors_total",
		Help: "Failed queries",
	}, []string{"backend", "storage"})
)
