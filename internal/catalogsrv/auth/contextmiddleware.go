package auth

import (
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

const (
	authHeaderPrefix = "Bearer "
	genericAuthError = "authentication failed"
)

// ContextMiddleware handles authentication and context setup for incoming requests
func ContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := log.Ctx(ctx)

		// Skip authentication for test contexts
		if catcommon.GetTestContext(ctx) {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			logger.Debug().Msg("missing authorization header")
			httpx.ErrUnAuthorized(genericAuthError).Send(w)
			return
		}

		if !strings.HasPrefix(authHeader, authHeaderPrefix) {
			logger.Debug().Msg("invalid authorization header format")
			httpx.ErrUnAuthorized(genericAuthError).Send(w)
			return
		}

		token := strings.TrimSpace(strings.TrimPrefix(authHeader, authHeaderPrefix))
		if token == "" {
			logger.Debug().Msg("empty token")
			httpx.ErrUnAuthorized(genericAuthError).Send(w)
			return
		}

		var err error
		ctx, err = ValidateToken(ctx, token)
		if err != nil {
			logger.Error().Err(err).Msg("token validation failed")
			httpx.ErrUnAuthorized(genericAuthError).Send(w)
			return
		}

		if config.Config().SingleUserMode {
			ctx = catcommon.WithProjectID(ctx, catcommon.ProjectId(config.Config().DefaultProjectID))
			logger.Debug().Str("project_id", config.Config().DefaultProjectID).Msg("using default project in single user mode")
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
