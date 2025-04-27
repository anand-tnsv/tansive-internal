package apis

import (
	"context"
	"net/url"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
)

func loadCatalogObject(ctx context.Context, c *common.CatalogContext, urlValues url.Values) (*common.CatalogContext, error) {
	if c.CatalogId == uuid.Nil && c.Catalog == "" {
		catalogId := getUrlValue(urlValues, "catalog_id")
		var err error
		if catalogId != "" {
			c.CatalogId, err = uuid.Parse(catalogId)
			if err != nil {
				return nil, err
			}
		} else {
			catalog := getUrlValue(urlValues, "catalog")
			if catalog != "" {
				c.Catalog = catalog
			}
		}
	}
	if c.CatalogId != uuid.Nil {
		if c.Catalog == "" {
			catalog, err := db.DB(ctx).GetCatalog(ctx, c.CatalogId, "")
			if err != nil {
				return nil, err
			}
			c.Catalog = catalog.Name
		}
	} else if c.Catalog != "" {
		catalog, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, c.Catalog)
		if err != nil {
			return nil, err
		}
		c.CatalogId = catalog.CatalogID
	}
	return c, nil
}

func loadVariantObject(ctx context.Context, c *common.CatalogContext, urlValues url.Values) (*common.CatalogContext, error) {
	if c.VariantId == uuid.Nil && c.Variant == "" {
		variantId := getUrlValue(urlValues, "variant_id")
		var err error
		if variantId != "" {
			c.VariantId, err = uuid.Parse(variantId)
			if err != nil {
				return nil, err
			}
		} else {
			variant := getUrlValue(urlValues, "variant")
			if variant != "" {
				c.Variant = variant
			}
		}
	}
	if c.VariantId != uuid.Nil {
		if c.Variant == "" {
			variant, err := db.DB(ctx).GetVariant(ctx, c.CatalogId, c.VariantId, "")
			if err != nil {
				return nil, err
			}
			c.Variant = variant.Name
		}
	} else if c.Variant != "" {
		variant, err := db.DB(ctx).GetVariant(ctx, c.CatalogId, uuid.Nil, c.Variant)
		if err != nil {
			return nil, err
		}
		c.VariantId = variant.VariantID
	}
	return c, nil
}

func loadWorkspaceObject(ctx context.Context, c *common.CatalogContext, urlValues url.Values) (*common.CatalogContext, error) {
	if c.WorkspaceId == uuid.Nil && c.WorkspaceLabel == "" {
		workspaceId := getUrlValue(urlValues, "workspace_id")
		var err error
		if workspaceId != "" {
			c.WorkspaceId, err = uuid.Parse(workspaceId)
			if err != nil {
				return nil, err
			}
		} else {
			workspaceLabel := getUrlValue(urlValues, "workspace")
			if workspaceLabel != "" {
				c.WorkspaceLabel = workspaceLabel
			}
		}
	}
	if c.WorkspaceId != uuid.Nil {
		if c.WorkspaceLabel == "" {
			workspace, err := db.DB(ctx).GetWorkspace(ctx, c.WorkspaceId)
			if err != nil {
				return nil, err
			}
			c.WorkspaceLabel = workspace.Label
		}
	} else if c.WorkspaceLabel != "" {
		workspace, err := db.DB(ctx).GetWorkspaceByLabel(ctx, c.VariantId, c.WorkspaceLabel)
		if err != nil {
			return nil, err
		}
		c.WorkspaceId = workspace.WorkspaceID
	}
	return c, nil
}

func loadNamespaceObject(ctx context.Context, c *common.CatalogContext, urlValues url.Values) (*common.CatalogContext, error) {
	var _ = ctx
	if c.Namespace == "" {
		c.Namespace = getUrlValue(urlValues, "namespace")
	}
	return c, nil
}

var key_shorthand = map[string]string{
	"catalog_id":   "c_id",
	"catalog":      "c",
	"variant_id":   "v_id",
	"variant":      "v",
	"workspace_id": "w_id",
	"workspace":    "w",
	"namespace":    "n",
}

func getUrlValue(urlValues url.Values, key string) string {
	v := urlValues.Get(key)
	if v == "" {
		key_shorthand, ok := key_shorthand[key]
		if ok {
			v = urlValues.Get(key_shorthand)
		}
	}
	return v
}
