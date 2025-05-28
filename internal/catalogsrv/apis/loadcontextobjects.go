package apis

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tidwall/gjson"
)

func loadContext(r *http.Request) (*catcommon.CatalogContext, error) {
	ctx := r.Context()

	catalogCtx := catcommon.GetCatalogContext(ctx)

	catalogCtx = loadMetadataFromParam(r, catalogCtx)
	catalogCtx = loadMetadataFromQuery(r, catalogCtx)

	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		var err error
		catalogCtx, err = loadMetadataFromBody(r, catalogCtx)
		if err != nil {
			return nil, err
		}
	}

	if catalogCtx.Catalog != "" {
		catalog, err := db.DB(ctx).GetCatalogByName(ctx, catalogCtx.Catalog)
		if err != nil {
			return nil, err
		}
		catalogCtx.CatalogId = catalog.CatalogID
	} else if catalogCtx.CatalogId != uuid.Nil {
		catalog, err := db.DB(ctx).GetCatalogByID(ctx, catalogCtx.CatalogId)
		if err != nil {
			return nil, err
		}
		catalogCtx.Catalog = catalog.Name
	}

	if catalogCtx.VariantId != uuid.Nil {
		if catalogCtx.Variant == "" {
			variant, err := db.DB(ctx).GetVariant(ctx, catalogCtx.CatalogId, catalogCtx.VariantId, "")
			if err != nil {
				return nil, err
			}
			catalogCtx.Variant = variant.Name
		}
	} else if catalogCtx.Variant != "" {
		variant, err := db.DB(ctx).GetVariant(ctx, catalogCtx.CatalogId, uuid.Nil, catalogCtx.Variant)
		if err != nil {
			return nil, err
		}
		catalogCtx.VariantId = variant.VariantID
	}

	return catalogCtx, nil
}

func loadMetadataFromParam(r *http.Request, catalogCtx *catcommon.CatalogContext) *catcommon.CatalogContext {
	if catalogCtx == nil {
		return nil
	}

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	namespace := chi.URLParam(r, "namespaceName")

	if catalogName != "" {
		catalogCtx.Catalog = catalogName
	}
	if variantName != "" {
		catalogCtx.Variant = variantName
	}
	if namespace != "" {
		catalogCtx.Namespace = namespace
	}

	return catalogCtx
}

func loadMetadataFromQuery(r *http.Request, catalogCtx *catcommon.CatalogContext) *catcommon.CatalogContext {
	if catalogCtx == nil {
		return nil
	}

	urlValues := r.URL.Query()

	if catalogCtx.CatalogId == uuid.Nil && catalogCtx.Catalog == "" {
		catalogID := getURLValue(urlValues, "catalog_id")
		if catalogID != "" {
			catalogUUID, err := uuid.Parse(catalogID)
			if err == nil {
				catalogCtx.CatalogId = catalogUUID
			}
		} else {
			catalog := getURLValue(urlValues, "catalog")
			if catalog != "" {
				catalogCtx.Catalog = catalog
			}
		}
	}

	if catalogCtx.VariantId == uuid.Nil && catalogCtx.Variant == "" {
		variantID := getURLValue(urlValues, "variant_id")
		if variantID != "" {
			variantUUID, err := uuid.Parse(variantID)
			if err == nil {
				catalogCtx.VariantId = variantUUID
			}
		} else {
			variant := getURLValue(urlValues, "variant")
			if variant != "" {
				catalogCtx.Variant = variant
			}
		}
	}

	if catalogCtx.Namespace == "" {
		namespace := getURLValue(urlValues, "namespace")
		if namespace != "" {
			catalogCtx.Namespace = namespace
		}
	}

	return catalogCtx
}

func loadMetadataFromBody(r *http.Request, catalogCtx *catcommon.CatalogContext) (*catcommon.CatalogContext, error) {
	if catalogCtx == nil || r.Body == nil {
		return catalogCtx, nil
	}
	w := httptest.NewRecorder() // we need a fake response writer
	r.Body = http.MaxBytesReader(w, r.Body, config.Config().MaxRequestBodySize)
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	// Restore body for downstream handlers using the buffered content
	r.Body = io.NopCloser(bytes.NewReader(body))

	// Parse metadata
	if catalogCtx.VariantId == uuid.Nil && catalogCtx.Variant == "" {
		if result := gjson.GetBytes(body, "metadata.variant"); result.Exists() {
			catalogCtx.Variant = result.String()
		}
	}
	if catalogCtx.Namespace == "" {
		if result := gjson.GetBytes(body, "metadata.namespace"); result.Exists() {
			catalogCtx.Namespace = result.String()
		}
	}

	return catalogCtx, nil
}

var urlKeyShorthand = map[string]string{
	"catalog_id": "c_id",
	"catalog":    "c",
	"variant_id": "v_id",
	"variant":    "v",
	"namespace":  "n",
}

// getURLValue retrieves a value from URL values, checking both full and shorthand keys
func getURLValue(values url.Values, key string) string {
	value := values.Get(key)
	if value == "" {
		if shorthand, ok := urlKeyShorthand[key]; ok {
			value = values.Get(shorthand)
		}
	}
	return value
}
