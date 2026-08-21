package api

import (
	"net/http"

	json "github.com/goccy/go-json"

	"prism/go-api/internal/model"
)

const (
	codeInvalidRequest      = "invalid_request"
	codeNotFound            = "not_found"
	codeStorageUnavailable  = "storage_unavailable"
	codeStorageError        = "storage_error"
)

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(model.ErrorBody{
		Error: model.ErrorDetail{Code: code, Message: message},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
