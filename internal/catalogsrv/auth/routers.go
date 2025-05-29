package auth

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var authHandlers = []policy.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/adopt-view/{catalogRef}/{viewLabel}",
		Handler: adoptView,
	},
	{
		Method:  http.MethodPost,
		Path:    "/adopt-default-view/{catalogRef}",
		Handler: adoptDefaultCatalogView,
	},
}

// Router creates and configures a new router for authentication-related endpoints.
// It sets up middleware and registers handlers for various HTTP methods and paths.
func Router(r chi.Router) chi.Router {
	router := chi.NewRouter()
	router.Use(UserAuthMiddleware)
	for _, handler := range authHandlers {
		router.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
	return router
}
