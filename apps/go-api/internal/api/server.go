package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

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
	r.Use(instrument(st.Name()))

	r.Get("/healthz", s.health)
	r.Get("/readyz", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/v1/meta", s.meta)
	r.Post("/v1/points", s.write)
	r.Post("/v1/query", s.queryPOST)
	r.Get("/v1/query", s.queryGET)
	r.Post("/v1/latest", s.latestPOST)
	r.Get("/v1/latest", s.latestGET)
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
		writeError(w, http.StatusServiceUnavailable, codeStorageUnavailable, err.Error())
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
		Contract: model.Contract,
		Ops:      model.Ops,
	})
}

func (s *Server) write(w http.ResponseWriter, r *http.Request) {
	var req model.WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	if len(req.Points) == 0 {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "points is required")
		return
	}
	now := time.Now().UTC()
	for i := range req.Points {
		if req.Points[i].Metric == "" {
			writeError(w, http.StatusBadRequest, codeInvalidRequest, "metric is required")
			return
		}
		if req.Points[i].TS.IsZero() {
			req.Points[i].TS = now
		}
		req.Points[i].Labels = model.NormalizeLabels(req.Points[i].Labels)
	}
	start := time.Now()
	err := s.store.Write(r.Context(), req.Points)
	metrics.ObserveBackend(s.store.Name(), "write", "http", len(req.Points), time.Since(start), err)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, model.WriteResponse{Written: len(req.Points)})
}

func (s *Server) queryPOST(w http.ResponseWriter, r *http.Request) {
	var req model.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	s.serveQuery(w, r, req)
}

func (s *Server) queryGET(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, err := time.Parse(time.RFC3339, q.Get("from"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "from must be RFC3339")
		return
	}
	to, err := time.Parse(time.RFC3339, q.Get("to"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "to must be RFC3339")
		return
	}
	labels, err := parseLabels(q.Get("labels"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "labels must be a JSON object")
		return
	}
	s.serveQuery(w, r, model.QueryRequest{
		Metric: q.Get("metric"),
		From:   from,
		To:     to,
		Step:   q.Get("step"),
		Agg:    q.Get("agg"),
		Labels: labels,
	})
}

func (s *Server) serveQuery(w http.ResponseWriter, r *http.Request, req model.QueryRequest) {
	if req.Metric == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "metric is required")
		return
	}
	if req.From.IsZero() || req.To.IsZero() {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "from and to are required")
		return
	}
	if req.Agg == "" {
		req.Agg = "avg"
	}
	if !model.ValidAgg(req.Agg) {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid agg")
		return
	}
	q := model.NormalizeQuery(req)
	start := time.Now()
	res, err := s.store.Query(r.Context(), q)
	items := 0
	if res != nil {
		items = len(res.Points)
		res.Step = q.StepRaw
	}
	metrics.ObserveBackend(s.store.Name(), "query", "http", items, time.Since(start), err)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) latestPOST(w http.ResponseWriter, r *http.Request) {
	var req model.LatestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	s.serveLatest(w, r, req)
}

func (s *Server) latestGET(w http.ResponseWriter, r *http.Request) {
	labels, err := parseLabels(r.URL.Query().Get("labels"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "labels must be a JSON object")
		return
	}
	s.serveLatest(w, r, model.LatestRequest{
		Metric: r.URL.Query().Get("metric"),
		Labels: labels,
	})
}

func (s *Server) serveLatest(w http.ResponseWriter, r *http.Request, req model.LatestRequest) {
	if req.Metric == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "metric is required")
		return
	}
	start := time.Now()
	p, err := s.store.Latest(r.Context(), req.Metric, model.NormalizeLabels(req.Labels))
	items := 0
	if p != nil {
		items = 1
	}
	metrics.ObserveBackend(s.store.Name(), "latest", "http", items, time.Since(start), err)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, codeNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
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
