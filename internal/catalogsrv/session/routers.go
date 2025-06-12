package session

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/apis"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var sessionTangentHandlers = []policy.ResponseHandlerParam{
	{
		Method:  http.MethodGet,
		Path:    "/execution-state/{sessionID}",
		Handler: getExecutionState,
	},
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
		r.Use(TangentContextMiddleware)
		for _, handler := range sessionTangentHandlers {
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

func TangentContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantID := config.Config().DefaultTenantID
		projectID := config.Config().DefaultProjectID
		ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))
		ctx = catcommon.WithProjectID(ctx, catcommon.ProjectId(projectID))
		log.Ctx(ctx).Info().Msgf("Setting tenant %s and project %s", tenantID, projectID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
