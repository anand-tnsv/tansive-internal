package apis

import (
	"context"
	"net/url"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
)

// loadCatalogObject loads catalog information into the context
func loadCatalogObject(ctx context.Context, catalogCtx *common.CatalogContext, values url.Values) (*common.CatalogContext, error) {
	if catalogCtx.CatalogId == uuid.Nil && catalogCtx.Catalog == "" {
		catalogID := getURLValue(values, "catalog_id")
		var err error
		if catalogID != "" {
			catalogCtx.CatalogId, err = uuid.Parse(catalogID)
			if err != nil {
				return nil, err
			}
		} else {
			catalog := getURLValue(values, "catalog")
			if catalog != "" {
				catalogCtx.Catalog = catalog
			}
		}
	}

	if catalogCtx.CatalogId != uuid.Nil {
		if catalogCtx.Catalog == "" {
			catalog, err := db.DB(ctx).GetCatalog(ctx, catalogCtx.CatalogId, "")
			if err != nil {
				return nil, err
			}
			catalogCtx.Catalog = catalog.Name
		}
	} else if catalogCtx.Catalog != "" {
		catalog, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, catalogCtx.Catalog)
		if err != nil {
			return nil, err
		}
		catalogCtx.CatalogId = catalog.CatalogID
	}
	return catalogCtx, nil
}

// loadVariantObject loads variant information into the context
func loadVariantObject(ctx context.Context, catalogCtx *common.CatalogContext, values url.Values) (*common.CatalogContext, error) {
	if catalogCtx.VariantId == uuid.Nil && catalogCtx.Variant == "" {
		variantID := getURLValue(values, "variant_id")
		var err error
		if variantID != "" {
			catalogCtx.VariantId, err = uuid.Parse(variantID)
			if err != nil {
				return nil, err
			}
		} else {
			variant := getURLValue(values, "variant")
			if variant != "" {
				catalogCtx.Variant = variant
			}
		}
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

// loadNamespaceObject loads namespace information into the context
func loadNamespaceObject(ctx context.Context, catalogCtx *common.CatalogContext, values url.Values) (*common.CatalogContext, error) {
	_ = ctx
	if catalogCtx.Namespace == "" {
		catalogCtx.Namespace = getURLValue(values, "namespace")
	}
	return catalogCtx, nil
}

var urlKeyShorthand = map[string]string{
	"catalog_id":   "c_id",
	"catalog":      "c",
	"variant_id":   "v_id",
	"variant":      "v",
	"workspace_id": "w_id",
	"workspace":    "w",
	"namespace":    "n",
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
