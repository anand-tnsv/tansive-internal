package auth

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func adoptView(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	catalogRef := chi.URLParam(r, "catalogRef")
	viewLabel := chi.URLParam(r, "viewLabel")

	var catalog *models.Catalog
	catalogId, err := uuid.Parse(catalogRef)
	if err != nil {
		catalog, err = db.DB(ctx).GetCatalog(ctx, uuid.Nil, catalogRef)
		if err != nil {
			return nil, httpx.ErrInvalidCatalog()
		}
	} else {
		catalog, err = db.DB(ctx).GetCatalog(ctx, catalogId, "")
		if err != nil {
			return nil, httpx.ErrInvalidCatalog()
		}
	}

	view, err := db.DB(ctx).GetViewByLabel(ctx, viewLabel, catalog.CatalogID)
	if err != nil {
		return nil, httpx.ErrInvalidView()
	}

	// get current context
	c := common.CatalogContextFromContext(ctx)

	_ = c
	_ = view

	return nil, nil
}
