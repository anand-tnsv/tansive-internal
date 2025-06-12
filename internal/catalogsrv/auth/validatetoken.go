package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
)

// setCatalogContext creates and configures a new CatalogContext
func setCatalogContext(ctx context.Context, viewDef *policy.ViewDefinition, tokenObj *Token) *catcommon.CatalogContext {
	_ = ctx
	catalogContext := &catcommon.CatalogContext{
		Catalog:   viewDef.Scope.Catalog,
		Variant:   viewDef.Scope.Variant,
		Namespace: viewDef.Scope.Namespace,
		CatalogID: tokenObj.GetCatalogID(),
	}

	sub := tokenObj.GetSubject()
	if strings.HasPrefix(sub, "user/") {
		catalogContext.UserContext = &catcommon.UserContext{
			UserID: strings.TrimPrefix(sub, "user/"),
		}
		catalogContext.Subject = catcommon.SubjectTypeUser
	} else if strings.HasPrefix(sub, "session/") {
		catalogContext.Subject = catcommon.SubjectTypeSession
	}

	return catalogContext
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

	ctx = policy.WithViewDefinition(ctx, &viewDef)
	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))

	catalogContext := setCatalogContext(ctx, &viewDef, tokenObj)
	ctx = catcommon.WithCatalogContext(ctx, catalogContext)

	return ctx, nil
}
