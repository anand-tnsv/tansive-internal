package auth

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/pkg/types"
)

// Custom errors for better error handling
var (
	ErrInvalidToken     = fmt.Errorf("invalid token")
	ErrInvalidViewRules = fmt.Errorf("invalid view rules")
	ErrMissingTenantID  = fmt.Errorf("missing tenant ID")
)

// setupCatalogContext creates and configures a new CatalogContext
func setupCatalogContext(ctx context.Context, viewDef *types.ViewDefinition, tokenObj *catalogmanager.Token) *catcommon.CatalogContext {
	_ = ctx
	catalogContext := &catcommon.CatalogContext{
		Catalog:   viewDef.Scope.Catalog,
		Variant:   viewDef.Scope.Variant,
		Namespace: viewDef.Scope.Namespace,
	}

	if tokenObj.GetTokenType() == catcommon.TokenTypeIdentity {
		catalogContext.UserContext = &catcommon.UserContext{
			UserID: tokenObj.GetSubject(),
		}
	}

	return catalogContext
}

// setupProjectContext adds project ID to context if in single user mode
func setupProjectContext(ctx context.Context) context.Context {
	if config.Config().SingleUserMode {
		return catcommon.WithProjectID(ctx, catcommon.ProjectId(config.Config().DefaultProjectID))
	}
	return ctx
}

// ValidateToken validates the provided token and sets up the appropriate context
func ValidateToken(ctx context.Context, token string) (context.Context, error) {
	if token == "" {
		return ctx, fmt.Errorf("%w: empty token", ErrInvalidToken)
	}

	tokenObj, err := catalogmanager.NewToken(ctx, token)
	if err != nil {
		return ctx, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	if !tokenObj.Validate() {
		return ctx, ErrInvalidToken
	}

	view := tokenObj.GetView()
	viewDef := types.ViewDefinition{}
	if err := json.Unmarshal(view.Rules, &viewDef); err != nil {
		return ctx, fmt.Errorf("%w: %v", ErrInvalidViewRules, err)
	}

	tenantID := tokenObj.GetTenantID()
	if tenantID == "" {
		return ctx, ErrMissingTenantID
	}

	ctx = WithViewDefinition(ctx, &viewDef)
	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))

	catalogContext := setupCatalogContext(ctx, &viewDef, tokenObj)
	ctx = catcommon.WithCatalogContext(ctx, catalogContext)

	ctx = setupProjectContext(ctx)

	return ctx, nil
}
