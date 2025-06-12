package session

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/apis"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var sessionHandlers = []policy.ResponseHandlerParam{
	{
		Method:  http.MethodGet,
		Path:    "/execution-state",
		Handler: getExecutionState,
	},
}

var sessionTangentHandlers = []policy.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/execution-state",
		Handler: createExecutionState,
	},
}

var sessionUserHandlers = []policy.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/",
		Handler: newSession,
	},
}

func Router() chi.Router {
	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(tangentAuthMiddleware)
		for _, handler := range sessionTangentHandlers {
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
		}
	})
	r.Group(func(r chi.Router) {
		r.Use(auth.ContextMiddleware)
		for _, handler := range sessionHandlers {
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
		}
	})
	r.Group(func(r chi.Router) {
		r.Use(auth.ContextMiddleware)
		r.Use(apis.CatalogContextLoader)
		for _, handler := range sessionUserHandlers {
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
		}
	})
	return r
}

func tangentAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}
