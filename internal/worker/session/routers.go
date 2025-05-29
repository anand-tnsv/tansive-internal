package session

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

type ResponseHandlerParam struct {
	Method  string
	Path    string
	Handler httpx.RequestHandler
}

var resourceObjectHandlers = []ResponseHandlerParam{
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
	// Route for connection that'll upgrade HTTP to WebSocket
	r.Method(http.MethodGet, "/{id}/channel", http.HandlerFunc(getSessionChannel))
}

func SessionAuthenticator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// implement authentication logic here
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
