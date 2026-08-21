package api

import (
	"bytes"
	"io"
	"net/http"
	"time"

	json "github.com/goccy/go-json"
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
	r.Get("/api/meta", s.meta)
	r.Get("/v1/meta", s.meta)
	r.Get("/api/tags", s.listTags)
	r.Post("/api/tags", s.upsertTags)
	r.Post("/api/values", s.readValues)
	r.Put("/api/values", s.write)
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

func decodeWriteItems(r io.Reader) ([]model.WriteItem, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, errEmpty
	}
	if raw[0] == '[' {
		var items []model.WriteItem
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, err
		}
		return items, nil
	}
	var wrap model.SamplesWrap
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, err
	}
	return wrap.Samples, nil
}

var errEmpty = io.EOF

func (s *Server) write(w http.ResponseWriter, r *http.Request) {
	items, err := decodeWriteItems(r.Body)
	if err != nil || len(items) == 0 {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "values array is required")
		return
	}
	now := time.Now().UTC()
	samples := make([]model.Sample, 0, len(items))
	for _, raw := range items {
		samples = append(samples, raw.Normalize(now))
	}
	start := time.Now()
	err = s.store.Write(r.Context(), samples)
	metrics.ObserveBackend(s.store.Name(), "write", "http", len(samples), time.Since(start), err)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, model.WriteResponse{Written: len(samples)})
}

func (s *Server) readValues(w http.ResponseWriter, r *http.Request) {
	var req model.ValuesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "invalid json")
		return
	}
	if len(req.TagsID) == 0 {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "tagsId is required")
		return
	}
	mode := req.Mode()
	start := time.Now()
	var (
		raw []model.Sample
		err error
	)
	switch mode {
	case "range":
		raw, err = s.store.Range(r.Context(), req.TagsID, req.Old.UTC(), req.Young.UTC())
	default:
		raw, err = s.store.Locf(r.Context(), req.TagsID, req.At())
	}
	metrics.ObserveBackend(s.store.Name(), mode, "http", len(raw), time.Since(start), err)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codeStorageError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, model.Assemble(req, raw))
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
