package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/common/httpx"
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
		if config.Config().SingleUserMode {
			tenantId := config.Config().DefaultTenantID
			projectId := config.Config().DefaultProjectID
			ctx = common.SetProjectIdInContext(
				common.SetTenantIdInContext(r.Context(), types.TenantId(tenantId)),
				types.ProjectId(projectId),
			)
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			httpx.ErrUnAuthorized("missing or invalid authorization token").Send(w)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		token = strings.TrimSpace(token) // just in case
		var err error
		ctx, err = validateToken(ctx, token)
		if err != nil {
			if config.Config().SingleUserMode {
				// in single user mode, we currently use a preset string. This is with the knowledge
				// it is insecure, but serves for experimentation.
				if token != config.Config().FakeSingleUserToken {
					httpx.ErrUnAuthorized("invalid authorization token").Send(w)
					return
				}
				ctx, err = setDefaultSingleUserContext(ctx)
				if err != nil {
					httpx.ErrUnAuthorized("invalid authorization token").Send(w)
					return
				}
			} else {
				httpx.ErrUnAuthorized("invalid authorization token").Send(w)
				return
			}
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func validateToken(ctx context.Context, token string) (context.Context, error) {
	// TODO: Implement token validation
	return ctx, fmt.Errorf("not implemented")
}

func setDefaultSingleUserContext(ctx context.Context) (context.Context, error) {
	// By default, we'll give admin view to all catalogs
	v := catalogmanager.ViewDefinition{
		Scope: catalogmanager.Scope{
			Catalog: "*",
		},
		Rules: []catalogmanager.Rule{
			{
				Intent: catalogmanager.IntentAllow,
				Actions: []catalogmanager.Action{
					catalogmanager.ActionCatalogAdmin,
				},
				Targets: []catalogmanager.TargetResource{
					"res://catalog/*",
				},
			},
		},
	}
	ctx = common.SetCatalogContext(ctx, &common.CatalogContext{
		ViewDefinition: v,
	})
	return ctx, nil
}
