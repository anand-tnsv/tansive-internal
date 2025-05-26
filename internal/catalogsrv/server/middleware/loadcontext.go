package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
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
			log.Ctx(ctx).Error().Err(err).Msg("invalid authorization token")
			httpx.ErrUnAuthorized("invalid authorization token").Send(w)
			return
		}

		if config.Config().SingleUserMode {
			ctx = common.SetProjectIdInContext(ctx, types.ProjectId(config.Config().DefaultProjectID))
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func validateToken(ctx context.Context, token string) (context.Context, error) {
	// validate
	tokenObj, err := catalogmanager.NewToken(ctx, token)
	if err != nil {
		return ctx, err
	}
	if !tokenObj.Validate() {
		return ctx, fmt.Errorf("invalid token")
	}
	view := tokenObj.GetView()
	viewDef := types.ViewDefinition{}
	if err := json.Unmarshal(view.Rules, &viewDef); err != nil {
		return ctx, fmt.Errorf("invalid view rules")
	}
	tenantID := tokenObj.GetTenantID()
	if tenantID == "" {
		return ctx, fmt.Errorf("invalid token")
	}
	ctx = common.SetTenantIdInContext(ctx, types.TenantId(tenantID))
	// Get the catalog context
	catalogContext := common.CatalogContextFromContext(ctx)
	if catalogContext == nil {
		catalogContext = &common.CatalogContext{}
	}
	catalogContext.ViewDefinition = &viewDef
	catalogContext.Catalog = viewDef.Scope.Catalog
	catalogContext.Variant = viewDef.Scope.Variant
	catalogContext.Namespace = viewDef.Scope.Namespace

	tokenType := tokenObj.GetTokenType()
	if tokenType == types.TokenTypeIdentity {
		catalogContext.UserContext = &common.UserContext{
			UserID: tokenObj.GetSubject(),
		}
	}
	return common.SetCatalogContext(ctx, catalogContext), nil
}
