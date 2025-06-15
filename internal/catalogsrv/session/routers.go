package session

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/apis"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
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
	{
		Method:  http.MethodPut,
		Path:    "/execution-state",
		Handler: updateExecutionState,
	},
}

var sessionUserHandlers = []policy.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/",
		Handler: newSession,
	},
	{
		Method:  http.MethodGet,
		Path:    "/",
		Handler: getSessions,
	},
	{
		Method:  http.MethodGet,
		Path:    "/{sessionID}",
		Handler: getSessionSummaryByID,
	},
	{
		Method: http.MethodGet,
		Path:   "/{sessionID}/auditlog",
		//		Handler: getAuditLogByID,
	},
}

func Router() chi.Router {
	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(tangentAuthMiddleware)
		r.Use(sessionContextMiddleware)
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

func sessionContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Skip authentication for test contexts
		if catcommon.GetTestContext(ctx) {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			log.Ctx(ctx).Debug().Msg("missing authorization header")
			// we follow through so it gets handled by the handlers
			next.ServeHTTP(w, r)
			return
		}

		if !strings.HasPrefix(authHeader, auth.AuthHeaderPrefix) {
			log.Ctx(ctx).Debug().Msg("invalid authorization header format")
			httpx.ErrUnAuthorized(auth.GenericAuthError).Send(w)
			return
		}

		token := strings.TrimSpace(strings.TrimPrefix(authHeader, auth.AuthHeaderPrefix))
		if token == "" {
			log.Ctx(ctx).Debug().Msg("empty token")
			httpx.ErrUnAuthorized(auth.GenericAuthError).Send(w)
			return
		}

		var err error
		ctx, err = auth.ValidateToken(ctx, token)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("token validation failed")
			httpx.ErrUnAuthorized(auth.GenericAuthError).Send(w)
			return
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
