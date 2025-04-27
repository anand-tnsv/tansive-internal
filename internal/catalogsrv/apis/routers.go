package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
)

var resourceObjectHandlers = []httpx.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/catalogs",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/catalogs/{catalogName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/catalogs/{catalogName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/catalogs/{catalogName}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/variants",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/variants/{variantName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/variants/{variantName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/variants/{variantName}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/workspaces",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/workspaces/{workspaceRef}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/workspaces/{workspaceRef}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/workspaces/{workspaceRef}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/namespaces",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/namespaces/{namespaceName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/namespaces/{namespaceName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/namespaces/{namespaceName}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/{objectType}",
		Handler: createObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/{objectType}/*",
		Handler: updateObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/{objectType}/*",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/{objectType}/*",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/{objectType}/*",
		Handler: deleteObject,
	},
}

func Router(r chi.Router) {
	r.Use(LoadCatalogContext)
	//TODO: Implement authentication
	for _, handler := range resourceObjectHandlers {
		r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
}

func LoadCatalogContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantId := common.TenantIdFromContext(ctx)
		projectId := common.ProjectIdFromContext(ctx)
		if tenantId == "" || projectId == "" {
			httpx.ErrInvalidRequest().Send(w)
			return
		}
		c := common.CatalogContextFromContext(ctx)
		if c == nil {
			ctx = common.SetCatalogContext(ctx, &common.CatalogContext{})
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		urlValues := r.URL.Query()
		// Load Catalog
		c, err := loadCatalogObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidCatalog().Send(w)
			return
		}

		// Load Variant
		c, err = loadVariantObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidVariant().Send(w)
			return
		}
		// Load Workspace
		c, err = loadWorkspaceObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidWorkspace().Send(w)
			return
		}

		// Load Namespace
		c, err = loadNamespaceObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidNamespace().Send(w)
			return
		}

		ctx = common.SetCatalogContext(ctx, c)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
