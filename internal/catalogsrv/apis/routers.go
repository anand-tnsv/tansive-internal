package apis

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/server/middleware"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/pkg/types"
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
		Method:       http.MethodGet,
		Path:         "/catalogs/{catalogName}",
		Handler:      getObject,
		PolicyAction: types.ActionCatalogList,
	},
	{
		Method:       http.MethodPut,
		Path:         "/catalogs/{catalogName}",
		Handler:      updateObject,
		PolicyAction: types.ActionCatalogAdmin,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/catalogs/{catalogName}",
		Handler:      deleteObject,
		PolicyAction: types.ActionCatalogAdmin,
	},
	{
		Method:       http.MethodPost,
		Path:         "/variants",
		Handler:      createObject,
		PolicyAction: types.ActionVariantClone,
	},
	{
		Method:       http.MethodGet,
		Path:         "/variants/{variantName}",
		Handler:      getObject,
		PolicyAction: types.ActionVariantList,
	},
	{
		Method:       http.MethodPut,
		Path:         "/variants/{variantName}",
		Handler:      updateObject,
		PolicyAction: types.ActionVariantAdmin,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/variants/{variantName}",
		Handler:      deleteObject,
		PolicyAction: types.ActionVariantAdmin,
	},
	{
		Method:       http.MethodPost,
		Path:         "/workspaces",
		Handler:      createObject,
		PolicyAction: types.ActionWorkspaceCreate,
	},
	{
		Method:       http.MethodGet,
		Path:         "/workspaces/{workspaceRef}",
		Handler:      getObject,
		PolicyAction: types.ActionWorkspaceList,
	},
	{
		Method:       http.MethodPut,
		Path:         "/workspaces/{workspaceRef}",
		Handler:      updateObject,
		PolicyAction: types.ActionWorkspaceAdmin,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/workspaces/{workspaceRef}",
		Handler:      deleteObject,
		PolicyAction: types.ActionWorkspaceAdmin,
	},
	{
		Method:       http.MethodPost,
		Path:         "/namespaces",
		Handler:      createObject,
		PolicyAction: types.ActionNamespaceCreate,
	},
	{
		Method:       http.MethodGet,
		Path:         "/namespaces/{namespaceName}",
		Handler:      getObject,
		PolicyAction: types.ActionNamespaceList,
	},
	{
		Method:       http.MethodPut,
		Path:         "/namespaces/{namespaceName}",
		Handler:      updateObject,
		PolicyAction: types.ActionNamespaceAdmin,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/namespaces/{namespaceName}",
		Handler:      deleteObject,
		PolicyAction: types.ActionNamespaceAdmin,
	},
	{
		Method:       http.MethodPost,
		Path:         "/views",
		Handler:      createObject,
		PolicyAction: types.ActionCatalogCreateView,
	},
	{
		Method:       http.MethodGet,
		Path:         "/views/{viewName}",
		Handler:      getObject,
		PolicyAction: types.ActionCatalogList,
	},
	{
		Method:       http.MethodPut,
		Path:         "/views/{viewName}",
		Handler:      updateObject,
		PolicyAction: types.ActionViewAdmin,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/views/{viewName}",
		Handler:      deleteObject,
		PolicyAction: types.ActionViewAdmin,
	},
	{
		Method:       http.MethodPost,
		Path:         "/collectionschemas",
		Handler:      createObject,
		PolicyAction: types.ActionSchemaCreate,
	},
	{
		Method:       http.MethodGet,
		Path:         "/collectionschemas/*",
		Handler:      getObject,
		PolicyAction: types.ActionSchemaRead,
	},
	{
		Method:       http.MethodPut,
		Path:         "/collectionschemas/*",
		Handler:      updateObject,
		PolicyAction: types.ActionSchemaEdit,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/collectionschemas/*",
		Handler:      deleteObject,
		PolicyAction: types.ActionSchemaEdit,
	},
	{
		Method:       http.MethodPost,
		Path:         "/parameterschemas",
		Handler:      createObject,
		PolicyAction: types.ActionSchemaCreate,
	},
	{
		Method:       http.MethodGet,
		Path:         "/parameterschemas/*",
		Handler:      getObject,
		PolicyAction: types.ActionSchemaRead,
	},
	{
		Method:       http.MethodPut,
		Path:         "/parameterschemas/*",
		Handler:      updateObject,
		PolicyAction: types.ActionSchemaEdit,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/parameterschemas/*",
		Handler:      deleteObject,
		PolicyAction: types.ActionSchemaEdit,
	},
	{
		Method:       http.MethodPost,
		Path:         "/collections",
		Handler:      createObject,
		PolicyAction: types.ActionSchemaInstantiate,
	},
	{
		Method:       http.MethodGet,
		Path:         "/collections/*",
		Handler:      getObject,
		PolicyAction: types.ActionCollectionRead,
	},
	{
		Method:       http.MethodPut,
		Path:         "/collections/*",
		Handler:      updateObject,
		PolicyAction: types.ActionCollectionWrite,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/collections/*",
		Handler:      deleteObject,
		PolicyAction: types.ActionCollectionWrite,
	},
	{
		Method:       http.MethodGet,
		Path:         "/attributes/*",
		Handler:      getObject,
		PolicyAction: types.ActionCollectionRead,
	},
	{
		Method:       http.MethodPost,
		Path:         "/attributes/*",
		Handler:      updateObject,
		PolicyAction: types.ActionCollectionWrite,
	},
	{
		Method:       http.MethodDelete,
		Path:         "/attributes/*",
		Handler:      deleteObject,
		PolicyAction: types.ActionCollectionWrite,
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
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(EnforceViewPolicy(handler)))
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

// EnforceViewPolicy is a middleware that enforces view-based access control policies.
// It validates that the request is allowed based on the view definition and policy rules.
func EnforceViewPolicy(handler httpx.ResponseHandlerParam) httpx.RequestHandler {
	return func(r *http.Request) (*httpx.Response, error) {
		ctx := r.Context()
		c := common.CatalogContextFromContext(ctx)
		if c == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}
		ourViewDef := c.ViewDefinition
		if ourViewDef == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}
		resourceName := getResourceNameFromPath(r)
		// Build the metadata path
		targetScope := types.Scope{
			Catalog:   c.Catalog,
			Variant:   c.Variant,
			Workspace: c.WorkspaceLabel,
			Namespace: c.Namespace,
		}
		targetResource := catalogmanager.MorphResource(targetScope, types.TargetResource("res://"+strings.TrimPrefix(r.URL.Path, "/")))
		if !strings.HasSuffix(string(targetResource), r.URL.Path) {
			return nil, httpx.ErrApplicationError("unable to validate policy for resource")
		}
		s := strings.Builder{}
		s.WriteString("res://")
		if c.Catalog != "" && resourceName != types.ResourceNameCatalogs {
			s.WriteString(types.ResourceNameCatalogs + "/" + c.Catalog)
		}
		if c.Variant != "" && resourceName != types.ResourceNameVariants {
			s.WriteString("/" + types.ResourceNameVariants + "/" + c.Variant)
		}
		if c.WorkspaceLabel != "" && resourceName != types.ResourceNameWorkspaces {
			s.WriteString("/" + types.ResourceNameWorkspaces + "/" + c.WorkspaceLabel)
		}
		if c.Namespace != "" && resourceName != types.ResourceNameNamespaces {
			s.WriteString("/" + types.ResourceNameNamespaces + "/" + c.Namespace)
		}
		metadata := s.String()

		// Build the policy request
		policyRequest := catalogmanager.PolicyRequest{
			ViewDefinition: ourViewDef,
			Metadata:       metadata,
			ResourceName:   resourceName,
			Action:         handler.PolicyAction,
			Target:         targetResource,
			Params:         r.URL.Query(),
		}

		if policyRequest.ResourceName == types.ResourceNameCollections && handler.PolicyAction == types.ActionSchemaInstantiate {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, httpx.ErrUnableToReadRequest()
			}
			// Create a new reader for downstream handlers
			r.Body = io.NopCloser(bytes.NewReader(body))
			policyRequest.ResourceJSON = body
		}

		// Validate the policy
		err := catalogmanager.ValidateViewPolicy(ctx, policyRequest)
		if err != nil {
			return nil, err
		}

		return handler.Handler(r)
	}
}
