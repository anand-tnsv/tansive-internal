package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func UserSessionValidator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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
				// it is insecure, but serves for local development.
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

		if config.Config().SingleUserMode {
			ctx = catcommon.SetProjectIdInContext(ctx, catcommon.ProjectId(config.Config().DefaultProjectID))
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func setDefaultSingleUserContext(ctx context.Context) (context.Context, error) {
	ctx = catcommon.SetTenantIdInContext(ctx, catcommon.TenantId(config.Config().DefaultTenantID))
	ctx = catcommon.SetProjectIdInContext(ctx, catcommon.ProjectId(config.Config().DefaultProjectID))
	catCtx := catcommon.CatalogContextFromContext(ctx)
	if catCtx == nil {
		catCtx = &catcommon.CatalogContext{}
	}
	if config.Config().SingleUserMode {
		catCtx.UserContext = &catcommon.UserContext{
			UserID: "default-user",
		}
	}
	ctx = catcommon.SetCatalogContext(ctx, catCtx)
	return ctx, nil
}
