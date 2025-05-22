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

func getResourceName(r *http.Request) (catalogmanager.RequestContext, error) {
	ctx := r.Context()

	var resourceName string

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	namespace := chi.URLParam(r, "namespaceName")
	workspace := chi.URLParam(r, "workspaceRef")
	viewName := chi.URLParam(r, "viewName")

	n := catalogmanager.RequestContext{}
	catalogContext := common.CatalogContextFromContext(ctx)
	if workspace != "" {
		resourceName = types.ResourceNameWorkspaces
		n.Workspace = workspace
		n.WorkspaceLabel, n.WorkspaceID = getUUIDOrName(workspace)
	} else if catalogContext != nil {
		n.WorkspaceLabel = catalogContext.WorkspaceLabel
		n.WorkspaceID = catalogContext.WorkspaceId
	}
	if variantName != "" {
		resourceName = types.ResourceNameVariants
		n.Variant, n.VariantID = getUUIDOrName(variantName)
	} else if catalogContext != nil {
		n.Variant = catalogContext.Variant
		n.VariantID = catalogContext.VariantId
	}
	if catalogName != "" {
		resourceName = types.ResourceNameCatalogs
		n.Catalog = catalogName
	} else if catalogContext != nil {
		n.Catalog = catalogContext.Catalog
		n.CatalogID = catalogContext.CatalogId
	}
	if namespace != "" {
		resourceName = types.ResourceNameNamespaces
		n.Namespace = namespace
	} else if catalogContext != nil {
		n.Namespace = catalogContext.Namespace
	}
	if viewName != "" {
		resourceName = types.ResourceNameViews
		n.ObjectName = viewName
	}

	n.QueryParams = r.URL.Query()

	if resourceName != "" {
		return n, nil
	}

	// parse schema and collection objects
	resourceName = getResourceNameFromPath(r)
	resourceFqn := chi.URLParam(r, "*")
	var objectName, objectPath string

	if resourceName != "" {
		if resourceFqn != "" {
			objectName = path.Base(resourceFqn)
			if objectName == "/" || objectName == "." {
				objectName = ""
			}

			// objectPath is the path without the last part
			objectPath = path.Dir(resourceFqn)
			if objectPath == "." {
				objectPath = "/"
			}
			objectPath = path.Clean("/" + objectPath) // this will always start with /
		}

		var catObjType types.CatalogObjectType
		if resourceName == types.ResourceNameCollectionSchemas {
			catObjType = types.CatalogObjectTypeCollectionSchema
		} else if resourceName == types.ResourceNameParameterSchemas {
			catObjType = types.CatalogObjectTypeParameterSchema
		} else if resourceName == types.ResourceNameCollections {
			catObjType = types.CatalogObjectTypeCatalogCollection
		}

		if n.ObjectName == "" {
			n.ObjectName = objectName
			n.ObjectPath = objectPath
			n.ObjectType = catObjType
		}

	}

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
	if kind == types.AttributeKind {
		if !gjson.GetBytes(reqJSON, "value").Exists() && !gjson.GetBytes(reqJSON, "values").Exists() {
			return httpx.ErrInvalidRequest("invalid request")
		}
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
