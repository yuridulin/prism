package api

import (
	"encoding/json"
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
	r.Get("/v1/tags", s.listTags)
	r.Post("/v1/tags", s.upsertTags)
	r.Post("/v1/write", s.write)
	r.Post("/v1/read", s.read)
	r.Post("/v1/locf", s.locf)
	r.Post("/v1/range", s.rangeOnly)
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
	if len(req.Samples) == 0 {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "samples is required")
		return
	}
	now := time.Now().UTC()
	samples := make([]model.Sample, 0, len(req.Samples))
	for _, raw := range req.Samples {
		samples = append(samples, raw.Normalize(now))
	}
	start := time.Now()
	err := s.store.Write(r.Context(), samples)
	metrics.ObserveBackend(s.store.Name(), "write", "http", len(samples), time.Since(start), err)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, model.WriteResponse{Written: len(samples)})
}

func (s *Server) locf(w http.ResponseWriter, r *http.Request) {
	var req model.ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	req.Mode = "locf"
	s.serveRead(w, r, req)
}

func (s *Server) rangeOnly(w http.ResponseWriter, r *http.Request) {
	var req model.ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	req.Mode = "range"
	s.serveRead(w, r, req)
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	var req model.ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	s.serveRead(w, r, req)
}

func (s *Server) serveRead(w http.ResponseWriter, r *http.Request, req model.ReadRequest) {
	if !model.ValidMode(req.Mode) {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "mode must be locf, range, sample or twavg")
		return
	}
	if len(req.TagIDs) == 0 {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "tag_ids is required")
		return
	}
	var (
		raw []model.Sample
		err error
	)
	start := time.Now()
	switch req.Mode {
	case "locf":
		if req.At.IsZero() {
			writeError(w, http.StatusBadRequest, codeInvalidRequest, "at is required")
			return
		}
		raw, err = s.store.Locf(r.Context(), req.TagIDs, req.At.UTC())
	default:
		if req.From.IsZero() || req.To.IsZero() {
			writeError(w, http.StatusBadRequest, codeInvalidRequest, "from and to are required")
			return
		}
		raw, err = s.store.Range(r.Context(), req.TagIDs, req.From.UTC(), req.To.UTC())
	}
	items := len(raw)
	metrics.ObserveBackend(s.store.Name(), req.Mode, "http", items, time.Since(start), err)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, model.Assemble(req.Mode, req, raw))
}

func (s *Server) listTags(w http.ResponseWriter, r *http.Request) {
	tags, err := s.store.ListTags(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	if tags == nil {
		tags = []model.Tag{}
	}
	writeJSON(w, http.StatusOK, model.TagList{Tags: tags})
}

func (s *Server) upsertTags(w http.ResponseWriter, r *http.Request) {
	var req model.TagWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	if len(req.Tags) == 0 {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "tags is required")
		return
	}
	if err := s.store.UpsertTags(r.Context(), req.Tags); err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, model.TagWriteResponse{Upserted: len(req.Tags)})
}
