package auth

import (
	"context"
	json "github.com/json-iterator/go"
	"fmt"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
)

// setCatalogContext creates and configures a new CatalogContext
func setCatalogContext(ctx context.Context, viewDef *policy.ViewDefinition, tokenObj *Token) *catcommon.CatalogContext {
	_ = ctx
	catalogContext := &catcommon.CatalogContext{
		Catalog:   viewDef.Scope.Catalog,
		Variant:   viewDef.Scope.Variant,
		Namespace: viewDef.Scope.Namespace,
	}

	if tokenObj.GetTokenUse() == IdentityTokenType {
		catalogContext.UserContext = &catcommon.UserContext{
			UserID: tokenObj.GetSubject(),
		}
	}

	return catalogContext
}

// setProjectContext adds project ID to context if in single user mode
func setProjectContext(ctx context.Context) context.Context {
	if config.Config().SingleUserMode {
		return catcommon.WithProjectID(ctx, catcommon.ProjectId(config.Config().DefaultProjectID))
	}
	return ctx
}

// ValidateToken validates the provided token and sets up the appropriate context
func ValidateToken(ctx context.Context, token string) (context.Context, error) {
	if token == "" {
		return ctx, ErrInvalidToken.Msg("empty token")
	}

	tokenObj, err := ParseAndValidateToken(ctx, token)
	if err != nil {
		return ctx, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	view := tokenObj.GetView()
	viewDef := policy.ViewDefinition{}
	if err := json.Unmarshal(view.Rules, &viewDef); err != nil {
		return ctx, fmt.Errorf("%w: %v", ErrInvalidViewRules, err)
	}

	tenantID := tokenObj.GetTenantID()
	if tenantID == "" {
		return ctx, ErrMissingTenantID
	}

	ctx = WithViewDefinition(ctx, &viewDef)
	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))

	catalogContext := setCatalogContext(ctx, &viewDef, tokenObj)
	ctx = catcommon.WithCatalogContext(ctx, catalogContext)

	ctx = setProjectContext(ctx)

	return ctx, nil
}
