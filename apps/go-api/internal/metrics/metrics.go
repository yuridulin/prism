// Package metrics is the three-layer telemetry surface:
//
//	api      — HTTP routes
//	backend  — application ops (HTTP or NATS)
//	storage  — adapter calls into the database
//
// Add a new op by using the same names in ObserveBackend / ObserveStorage.
// Native DB exporters are scraped separately by Prometheus.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"prism/go-api/internal/apperr"
)

const Backend = "go"

var (
	APIRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_api_requests_total",
		Help: "HTTP requests by route",
	}, []string{"backend", "storage", "route", "method", "status"})

	APIDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "prism_api_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"backend", "storage", "route", "method"})

	BackendOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_backend_ops_total",
		Help: "Application operations",
	}, []string{"backend", "storage", "op", "source", "result"})

	BackendDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "prism_backend_op_duration_seconds",
		Help:    "Application operation latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"backend", "storage", "op", "source"})

	BackendItems = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_backend_items_total",
		Help: "Points written or samples returned",
	}, []string{"backend", "storage", "op", "source"})

	StorageOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prism_storage_ops_total",
		Help: "Storage adapter operations",
	}, []string{"backend", "storage", "op", "result"})

	StorageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "prism_storage_op_duration_seconds",
		Help:    "Storage adapter latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"backend", "storage", "op"})

	StorageUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "prism_storage_up",
		Help: "1 if the last storage ping succeeded",
	}, []string{"backend", "storage"})
)

func ObserveBackend(storage, op, source string, items int, d time.Duration, err error) {
	BackendOps.WithLabelValues(Backend, storage, op, source, apperr.Result(err)).Inc()
	BackendDuration.WithLabelValues(Backend, storage, op, source).Observe(d.Seconds())
	if err == nil && items > 0 {
		BackendItems.WithLabelValues(Backend, storage, op, source).Add(float64(items))
	}
}

func ObserveStorage(storage, op string, d time.Duration, err error) {
	StorageOps.WithLabelValues(Backend, storage, op, apperr.Result(err)).Inc()
	StorageDuration.WithLabelValues(Backend, storage, op).Observe(d.Seconds())
	if op == "ping" {
		up := 0.0
		if err == nil {
			up = 1
		}
		StorageUp.WithLabelValues(Backend, storage).Set(up)
	}
}

func ObserveAPI(storage, route, method, status string, d time.Duration) {
	APIRequests.WithLabelValues(Backend, storage, route, method, status).Inc()
	APIDuration.WithLabelValues(Backend, storage, route, method).Observe(d.Seconds())
}
