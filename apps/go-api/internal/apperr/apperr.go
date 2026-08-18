package apperr

import "errors"

var ErrNotFound = errors.New("not found")

func Result(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrNotFound) {
		return "not_found"
	}
	return "error"
}
