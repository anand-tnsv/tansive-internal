package auth

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var authHandlers = []httpx.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/adopt-view/{catalogRef}/{viewLabel}",
		Handler: adoptView,
	},
}

func Router(r chi.Router) {
	r.Use(sessionValidator)
	for _, handler := range authHandlers {
		r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
}

func sessionValidator(next http.Handler) http.Handler {
	// We'll validate login sessions here. For now, we'll just pass through.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}
