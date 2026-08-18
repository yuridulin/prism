package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"prism/go-api/internal/metrics"
	"prism/go-api/internal/model"
	"prism/go-api/internal/store"
)

type Server struct {
	store store.Store
	mux   http.Handler
}

func New(st store.Store) *Server {
	s := &Server{store: st}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", s.health)
	r.Get("/readyz", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/v1/meta", s.meta)
	r.Post("/v1/points", s.write)
	r.Get("/v1/query", s.query)
	r.Get("/v1/latest", s.latest)
	s.mux = r
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func (s *Server) meta(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, model.Meta{
		Backend:  "go",
		Storage:  s.store.Name(),
		Storages: store.Supported,
	})
}

func (s *Server) write(w http.ResponseWriter, r *http.Request) {
	var req model.WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if len(req.Points) == 0 {
		http.Error(w, "points is required", http.StatusBadRequest)
		return
	}
	now := time.Now().UTC()
	for i := range req.Points {
		if req.Points[i].Metric == "" {
			http.Error(w, "metric is required", http.StatusBadRequest)
			return
		}
		if req.Points[i].TS.IsZero() {
			req.Points[i].TS = now
		}
		req.Points[i].Labels = model.NormalizeLabels(req.Points[i].Labels)
	}
	if err := s.store.Write(r.Context(), req.Points); err != nil {
		metrics.IngestErrors.WithLabelValues("go", s.store.Name()).Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.IngestPoints.WithLabelValues("go", s.store.Name()).Add(float64(len(req.Points)))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		http.Error(w, "metric is required", http.StatusBadRequest)
		return
	}
	from, err := time.Parse(time.RFC3339, q.Get("from"))
	if err != nil {
		http.Error(w, "from must be RFC3339", http.StatusBadRequest)
		return
	}
	to, err := time.Parse(time.RFC3339, q.Get("to"))
	if err != nil {
		http.Error(w, "to must be RFC3339", http.StatusBadRequest)
		return
	}
	step, err := time.ParseDuration(q.Get("step"))
	if err != nil || step <= 0 {
		step = time.Minute
	}
	agg := q.Get("agg")
	if agg == "" {
		agg = "avg"
	}
	if !model.ValidAgg(agg) {
		http.Error(w, "invalid agg", http.StatusBadRequest)
		return
	}
	labels, err := parseLabels(q.Get("labels"))
	if err != nil {
		http.Error(w, "labels must be a JSON object", http.StatusBadRequest)
		return
	}

	timer := prometheusTimer("go", s.store.Name())
	defer timer()

	res, err := s.store.Query(r.Context(), model.Query{
		Metric: metric,
		From:   from,
		To:     to,
		Step:   step,
		Agg:    agg,
		Labels: labels,
	})
	if err != nil {
		metrics.QueryErrors.WithLabelValues("go", s.store.Name()).Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) latest(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric is required", http.StatusBadRequest)
		return
	}
	labels, err := parseLabels(r.URL.Query().Get("labels"))
	if err != nil {
		http.Error(w, "labels must be a JSON object", http.StatusBadRequest)
		return
	}
	timer := prometheusTimer("go", s.store.Name())
	defer timer()
	p, err := s.store.Latest(r.Context(), metric, labels)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		metrics.QueryErrors.WithLabelValues("go", s.store.Name()).Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func prometheusTimer(backend, storage string) func() {
	start := time.Now()
	return func() {
		metrics.QueryDuration.WithLabelValues(backend, storage).Observe(time.Since(start).Seconds())
	}
}

func parseLabels(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(raw), &labels); err != nil {
		return nil, err
	}
	return labels, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
