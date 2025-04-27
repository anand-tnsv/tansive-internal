package middleware

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/pkg/types"
)

func LoadContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if common.TestContextFromContext(ctx) {
			// If the context is already set, skip loading it again
			next.ServeHTTP(w, r)
			return
		}
		tenantId := chi.URLParam(r, "tenantId")
		projectId := chi.URLParam(r, "projectId")
		r = r.WithContext(
			common.SetProjectIdInContext(
				common.SetTenantIdInContext(r.Context(), types.TenantId(tenantId)),
				types.ProjectId(projectId),
			),
		)
		next.ServeHTTP(w, r)
	})
}
