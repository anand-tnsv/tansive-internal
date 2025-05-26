package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/pkg/types"
)

type adoptViewRsp struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// adoptView adopts a view from a catalog. The parent view must be scoped to the catalog and
// the derived view must have a policy subset of the parent view.
func adoptView(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	catalogRef := chi.URLParam(r, "catalogRef")
	viewLabel := chi.URLParam(r, "viewLabel")

	var catalog *models.Catalog
	catalogID, err := uuid.Parse(catalogRef)
	if err != nil {
		catalog, err = db.DB(ctx).GetCatalog(ctx, uuid.Nil, catalogRef)
		if err != nil {
			return nil, httpx.ErrInvalidCatalog()
		}
	} else {
		catalog, err = db.DB(ctx).GetCatalog(ctx, catalogID, "")
		if err != nil {
			return nil, httpx.ErrInvalidCatalog()
		}
	}

	// get current context
	c := common.CatalogContextFromContext(ctx)
	ourViewDef := c.ViewDefinition
	if ourViewDef == nil || ourViewDef.Scope.Catalog != catalog.Name {
		return nil, httpx.ErrInvalidView("current view not in catalog: " + catalog.Name)
	}

	wantView, err := db.DB(ctx).GetViewByLabel(ctx, viewLabel, catalog.CatalogID)
	if err != nil {
		return nil, httpx.ErrInvalidView()
	}

	token, tokenExpiry, err := catalogmanager.CreateToken(ctx, wantView, catalogmanager.WithParentViewDefinition(ourViewDef))
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response: &adoptViewRsp{
			Token:     token,
			ExpiresAt: tokenExpiry,
		},
	}

	return rsp, nil
}

// adoptDefaultCatalogView adopts the default view for a catalog.
func adoptDefaultCatalogView(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	catalogRef := chi.URLParam(r, "catalogRef")

	var catalog *models.Catalog
	catalogID, err := uuid.Parse(catalogRef)
	if err != nil {
		catalog, err = db.DB(ctx).GetCatalog(ctx, uuid.Nil, catalogRef)
		if err != nil {
			return nil, httpx.ErrInvalidCatalog()
		}
	} else {
		catalog, err = db.DB(ctx).GetCatalog(ctx, catalogID, "")
		if err != nil {
			return nil, httpx.ErrInvalidCatalog()
		}
	}

	// get current context
	wantView, err := getDefaultUserViewDefInCatalog(ctx, catalog.CatalogID)
	if err != nil {
		return nil, err
	}

	viewDef := types.ViewDefinition{}
	if err = json.Unmarshal(wantView.Rules, &viewDef); err != nil {
		return nil, httpx.ErrInvalidView("invalid view: " + wantView.Label)
	}

	//the parent view is the default view for the catalog
	token, tokenExpiry, err := catalogmanager.CreateToken(ctx,
		wantView,
		catalogmanager.WithParentViewDefinition(&viewDef),
		catalogmanager.WithAdditionalClaims(map[string]any{
			"token_type": types.TokenTypeIdentity,
			"sub":        common.GetUserContextFromContext(ctx).UserID,
		}),
	)
	if err != nil {
		return nil, httpx.ErrInvalidView()
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response: &adoptViewRsp{
			Token:     token,
			ExpiresAt: tokenExpiry,
		},
	}

	return rsp, nil
}

func getDefaultUserViewDefInCatalog(ctx context.Context, catalogID uuid.UUID) (*models.View, error) {
	// get usercontext
	userContext := common.GetUserContextFromContext(ctx)
	if userContext == nil {
		return nil, httpx.ErrUnAuthorized()
	}
	if userContext.UserID == "" {
		return nil, httpx.ErrUnAuthorized()
	}
	// Currently in single user mode, return admin view
	v, err := db.DB(ctx).GetViewByLabel(ctx, types.DefaultAdminViewLabel, catalogID)
	if err != nil {
		return nil, httpx.ErrUnableToServeRequest()
	}
	return v, nil
}
