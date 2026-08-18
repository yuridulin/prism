package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"prism/go-api/internal/metrics"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func instrument(storage string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skipAPIMetrics(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(sw, r)
			route := routeLabel(r)
			metrics.ObserveAPI(storage, route, r.Method, strconv.Itoa(sw.status), time.Since(start))
		})
	}
}

func skipAPIMetrics(path string) bool {
	return path == "/metrics" || path == "/healthz" || path == "/readyz"
}

func routeLabel(r *http.Request) string {
	if ctx := chi.RouteContext(r.Context()); ctx != nil {
		if p := ctx.RoutePattern(); p != "" {
			p = strings.TrimPrefix(p, "/")
			p = strings.ReplaceAll(p, "/", "_")
			return p
		}
	}
	return "other"
}
