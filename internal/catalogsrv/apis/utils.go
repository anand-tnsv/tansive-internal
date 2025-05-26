package apis

import (
	"net/http"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/gjson"
)

func hydrateRequestContext(r *http.Request, body []byte) (catalogmanager.RequestContext, error) {
	ctx := r.Context()

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	namespace := chi.URLParam(r, "namespaceName")
	viewName := chi.URLParam(r, "viewName")
	resourcePath := chi.URLParam(r, "resourcePath")
	resourceValue := chi.URLParam(r, "resourceValue")

	n := catalogmanager.RequestContext{}
	catalogContext := common.CatalogContextFromContext(ctx)
	if variantName != "" {
		n.Variant, n.VariantID = getUUIDOrName(variantName)
	} else if catalogContext != nil {
		n.Variant = catalogContext.Variant
		n.VariantID = catalogContext.VariantId
	}
	if catalogName != "" {
		n.Catalog = catalogName
	} else if catalogContext != nil {
		n.Catalog = catalogContext.Catalog
		n.CatalogID = catalogContext.CatalogId
	}
	if namespace != "" {
		n.Namespace = namespace
	} else if catalogContext != nil {
		n.Namespace = catalogContext.Namespace
	}

	hydrateObjectMetadata(&n, body)

	if viewName != "" {
		n.ObjectName = viewName
	}

	if resourcePath != "" {
		n.ObjectName = path.Base(resourcePath)
		if n.ObjectName == "/" || n.ObjectName == "." {
			n.ObjectName = ""
		}
		n.ObjectPath = path.Dir(resourcePath)
		if n.ObjectPath == "." {
			n.ObjectPath = "/"
		}
		n.ObjectPath = path.Clean("/" + n.ObjectPath)
		n.ObjectType = types.CatalogObjectTypeResource
		n.ObjectProperty = types.ResourcePropertyDefinition
	}

	if resourceValue != "" {
		n.ObjectName = path.Base(resourceValue)
		if n.ObjectName == "/" || n.ObjectName == "." {
			n.ObjectName = ""
		}
		n.ObjectPath = path.Dir(resourceValue)
		if n.ObjectPath == "." {
			n.ObjectPath = "/"
		}
		n.ObjectPath = path.Clean("/" + n.ObjectPath)
		n.ObjectType = types.CatalogObjectTypeResource
		n.ObjectProperty = types.ResourcePropertyValue
	}

	n.QueryParams = r.URL.Query()

	return n, nil
}

func getResourceKind(r *http.Request) string {
	return types.KindFromResourceName(getResourceNameFromPath(r))
}

func getResourceNameFromPath(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.Split(path, "/")
	var resourceName string
	if len(segments) > 0 {
		resourceName = segments[0]
	}
	return resourceName
}

func getUUIDOrName(ref string) (string, uuid.UUID) {
	if ref == "" {
		return "", uuid.Nil
	}
	u, err := uuid.Parse(ref)
	if err != nil {
		return ref, uuid.Nil
	}
	return "", u
}

func validateRequest(reqJSON []byte, kind string) error {
	if !gjson.ValidBytes(reqJSON) {
		return httpx.ErrInvalidRequest("unable to parse request")
	}
	if kind == types.ResourceKind {
		return nil
	}
	result := gjson.GetBytes(reqJSON, "kind")
	if !result.Exists() {
		return httpx.ErrInvalidRequest("missing kind")
	}
	if result.String() != kind {
		return httpx.ErrInvalidRequest("invalid kind")
	}
	return nil
}

func hydrateObjectMetadata(n *catalogmanager.RequestContext, body []byte) {
	result := gjson.GetBytes(body, "metadata")
	if !result.Exists() {
		return
	}
	val := result.Value()

	metadata, ok := val.(map[string]interface{})
	if !ok {
		return
	}
	if n.Catalog == "" && n.CatalogID == uuid.Nil {
		catalog, ok := metadata["catalog"].(string)
		if ok {
			n.Catalog = catalog
		}
	}
	if n.Variant == "" && n.VariantID == uuid.Nil {
		variant, ok := metadata["variant"].(string)
		if ok {
			n.Variant = variant
		}
	}
	if n.Namespace == "" {
		namespace, ok := metadata["namespace"].(string)
		if ok {
			n.Namespace = namespace
		}
	}
}
