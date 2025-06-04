package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

// adoptViewRsp represents the response structure for view adoption operations
type adoptViewRsp struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// getCatalogByRef retrieves a catalog by either its ID or name
func getCatalogByRef(ctx context.Context, catalogRef string) (*models.Catalog, apperrors.Error) {
	catalogID, err := uuid.Parse(catalogRef)
	if err != nil {
		return db.DB(ctx).GetCatalogByName(ctx, catalogRef)
	}
	return db.DB(ctx).GetCatalogByID(ctx, catalogID)
}

// adoptView adopts a view from a catalog. The parent view must be scoped to the catalog and
// the derived view must have a policy subset of the parent view.
func adoptView(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	catalogRef := chi.URLParam(r, "catalogRef")
	viewLabel := chi.URLParam(r, "viewLabel")

	catalog, err := getCatalogByRef(ctx, catalogRef)
	if err != nil {
		return nil, ErrCatalogNotFound.Err(err)
	}

	// Validate current context
	ourViewDef := policy.GetViewDefinition(ctx)
	if ourViewDef == nil {
		return nil, ErrInvalidView.Msg("no current view definition found")
	}
	if ourViewDef.Scope.Catalog != catalog.Name {
		return nil, ErrInvalidView.Msg("current view not in catalog: " + catalog.Name)
	}

	wantView, err := db.DB(ctx).GetViewByLabel(ctx, viewLabel, catalog.CatalogID)
	if err != nil {
		return nil, ErrViewNotFound.Err(err)
	}

	token, tokenExpiry, err := CreateToken(ctx,
		wantView,
		WithParentViewDefinition(ourViewDef),
		WithAdditionalClaims(getAccessTokenClaims(ctx)),
	)
	if err != nil {
		return nil, ErrTokenGeneration.Msg(err.Error())
	}

	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response: &adoptViewRsp{
			Token:     token,
			ExpiresAt: tokenExpiry,
		},
	}, nil
}

// adoptDefaultCatalogView adopts the default view for a catalog.
func adoptDefaultCatalogView(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	catalogRef := chi.URLParam(r, "catalogRef")

	catalog, err := getCatalogByRef(ctx, catalogRef)
	if err != nil {
		return nil, ErrCatalogNotFound.Err(err)
	}

	wantView, err := getDefaultUserViewDefInCatalog(ctx, catalog.CatalogID)
	if err != nil {
		return nil, err
	}

	viewDef := policy.ViewDefinition{}
	if err := json.Unmarshal(wantView.Rules, &viewDef); err != nil {
		return nil, ErrInvalidView.Msg("invalid view definition").Err(err)
	}

	userContext := catcommon.GetUserContext(ctx)
	if userContext == nil || userContext.UserID == "" {
		return nil, ErrUnauthorized
	}

	token, tokenExpiry, err := CreateToken(ctx,
		wantView,
		WithParentViewDefinition(&viewDef),
		WithAdditionalClaims(getAccessTokenClaims(ctx)),
	)
	if err != nil {
		return nil, ErrTokenGeneration.Err(err)
	}

	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response: &adoptViewRsp{
			Token:     token,
			ExpiresAt: tokenExpiry,
		},
	}, nil
}

// getDefaultUserViewDefInCatalog retrieves the default view definition for a user in a catalog.
func getDefaultUserViewDefInCatalog(ctx context.Context, catalogID uuid.UUID) (*models.View, apperrors.Error) {
	userContext := catcommon.GetUserContext(ctx)
	if userContext == nil || userContext.UserID == "" {
		return nil, ErrUnauthorized
	}

	// Currently in single user mode, return admin view
	v, err := db.DB(ctx).GetViewByLabel(ctx, catcommon.DefaultAdminViewLabel, catalogID)
	if err != nil {
		return nil, ErrViewNotFound.Err(err)
	}
	return v, nil
}

func getIdentityTokenClaims(ctx context.Context) map[string]any {
	userContext := catcommon.GetUserContext(ctx)
	if userContext == nil || userContext.UserID == "" {
		return nil
	}

	return map[string]any{
		"token_use": IdentityTokenType,
		"sub":       "user/" + userContext.UserID,
	}
}

var _ = getIdentityTokenClaims

func getAccessTokenClaims(ctx context.Context) map[string]any {
	var subject string
	userContext := catcommon.GetUserContext(ctx)
	if userContext == nil || userContext.UserID == "" {
		return nil
	}
	subject = "user/" + userContext.UserID

	return map[string]any{
		"token_use": AccessTokenType,
		"sub":       subject,
	}
}
