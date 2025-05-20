package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/server/middleware"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var userSessionHandlers = []httpx.ResponseHandlerParam{
	{
		Method:  http.MethodGet,
		Path:    "/views",
		Handler: listObjects,
	},
	{
		Method:  http.MethodPost,
		Path:    "/catalogs",
		Handler: createObject,
	},
}
var resourceObjectHandlers = []httpx.ResponseHandlerParam{
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
		Path:    "/views",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/views/{viewName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/views/{viewName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/views/{viewName}",
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

// Router creates and configures a new router for catalog service API endpoints.
// It sets up middleware and registers handlers for various HTTP methods and paths.
func Router(r chi.Router) chi.Router {
	router := chi.NewRouter()
	//Load the group that needs only user session/identity validation
	router.Group(func(r chi.Router) {
		r.Use(middleware.UserSessionValidator)
		r.Use(LoadCatalogContext)
		for _, handler := range userSessionHandlers {
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
		}
	})
	//Load the group that needs session validation and catalog context
	router.Group(func(r chi.Router) {
		r.Use(middleware.LoadContext)
		r.Use(LoadCatalogContext)
		for _, handler := range resourceObjectHandlers {
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
		}
	})
	return router
}

// LoadCatalogContext is a middleware that loads and validates catalog context information
// from the request context and URL parameters. It ensures that tenant and project IDs
// are present and loads related objects (catalog, variant, workspace, namespace).
func LoadCatalogContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantID := common.TenantIdFromContext(ctx)
		projectID := common.ProjectIdFromContext(ctx)
		if tenantID == "" || projectID == "" {
			httpx.ErrInvalidRequest().Send(w)
			return
		}
		c := common.CatalogContextFromContext(ctx)
		if c == nil {
			httpx.ErrUnAuthorized("missing or invalid authorization token").Send(w)
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
