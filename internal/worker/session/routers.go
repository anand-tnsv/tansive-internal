package session

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var resourceObjectHandlers = []httpx.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/",
		Handler: createSession,
	},
	{
		Method:  http.MethodGet,
		Path:    "/{id}",
		Handler: getSession,
	},
	{
		Method:  http.MethodGet,
		Path:    "/",
		Handler: listSessions,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/{id}",
		Handler: deleteSession,
	},
}

func Router(r chi.Router) {
	r.Use(SessionAuthenticator)
	for _, handler := range resourceObjectHandlers {
		r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
}

func SessionAuthenticator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// implement authentication logic here
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
