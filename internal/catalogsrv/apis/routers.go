package apis

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
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

// resourceObjectHandlers defines the API routes and their authorization requirements.
// Each route requires at least one of the listed actions to be authorized.
var resourceObjectHandlers = []httpx.ResponseHandlerParam{
	{
		Method:         http.MethodGet,
		Path:           "/catalogs/{catalogName}",
		Handler:        getObject,
		AllowedActions: []types.Action{types.ActionCatalogList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/catalogs/{catalogName}",
		Handler:        updateObject,
		AllowedActions: []types.Action{types.ActionCatalogAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/catalogs/{catalogName}",
		Handler:        deleteObject,
		AllowedActions: []types.Action{types.ActionCatalogAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/variants",
		Handler:        createObject,
		AllowedActions: []types.Action{types.ActionVariantClone},
	},
	{
		Method:         http.MethodGet,
		Path:           "/variants/{variantName}",
		Handler:        getObject,
		AllowedActions: []types.Action{types.ActionVariantList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/variants/{variantName}",
		Handler:        updateObject,
		AllowedActions: []types.Action{types.ActionVariantAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/variants/{variantName}",
		Handler:        deleteObject,
		AllowedActions: []types.Action{types.ActionVariantAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/namespaces",
		Handler:        createObject,
		AllowedActions: []types.Action{types.ActionNamespaceCreate},
	},
	{
		Method:         http.MethodGet,
		Path:           "/namespaces/{namespaceName}",
		Handler:        getObject,
		AllowedActions: []types.Action{types.ActionNamespaceList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/namespaces/{namespaceName}",
		Handler:        updateObject,
		AllowedActions: []types.Action{types.ActionNamespaceAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/namespaces/{namespaceName}",
		Handler:        deleteObject,
		AllowedActions: []types.Action{types.ActionNamespaceAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/views",
		Handler:        createObject,
		AllowedActions: []types.Action{types.ActionCatalogCreateView},
	},
	{
		Method:         http.MethodGet,
		Path:           "/views/{viewName}",
		Handler:        getObject,
		AllowedActions: []types.Action{types.ActionCatalogList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/views/{viewName}",
		Handler:        updateObject,
		AllowedActions: []types.Action{types.ActionViewAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/views/{viewName}",
		Handler:        deleteObject,
		AllowedActions: []types.Action{types.ActionViewAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/resources",
		Handler:        createObject,
		AllowedActions: []types.Action{types.ActionResourceCreate},
	},
	{
		Method:         http.MethodGet,
		Path:           "/resources",
		Handler:        listObjects,
		AllowedActions: []types.Action{types.ActionResourceList},
	},
	{
		Method:         http.MethodGet,
		Path:           "/resources/{resourcePath:.+}/definition",
		Handler:        getObject,
		AllowedActions: []types.Action{types.ActionResourceRead, types.ActionResourceEdit},
	},
	{
		Method:         http.MethodGet,
		Path:           "/resources/{resourceValue:.+}",
		Handler:        getObject,
		AllowedActions: []types.Action{types.ActionResourceGet, types.ActionResourcePut},
	},
	{
		Method:         http.MethodPut,
		Path:           "/resources/{resourceValue:.+}",
		Handler:        updateObject,
		AllowedActions: []types.Action{types.ActionResourcePut},
	},
	{
		Method:         http.MethodPut,
		Path:           "/resources/{resourcePath:.+}/definition",
		Handler:        updateObject,
		AllowedActions: []types.Action{types.ActionResourceEdit},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/resources/{resourcePath:.+}/definition",
		Handler:        deleteObject,
		AllowedActions: []types.Action{types.ActionResourceDelete},
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
			//Wrap the request handler with view policy enforcement
			policyEnforcedHandler := EnforceViewPolicy(handler)
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(policyEnforcedHandler))
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
		c, err := loadContext(r)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				log.Ctx(ctx).Error().Msgf("request body too large (limit: %d bytes)", maxErr.Limit)
				httpx.ErrRequestTooLarge(maxErr.Limit).Send(w)
			} else {
				httpx.ErrInvalidRequest(err.Error()).Send(w)
			}
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

		//Get allowed actions from Context. This is set from the token by validation middleware
		c := common.CatalogContextFromContext(ctx)
		if c == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}
		authorizedViewDef := catalogmanager.CanonicalizeViewDefinition(c.ViewDefinition)
		if authorizedViewDef == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}

		// Build the metadata path
		targetScope := types.Scope{
			Catalog:   c.Catalog,
			Variant:   c.Variant,
			Namespace: c.Namespace,
		}
		targetResource := catalogmanager.CanonicalizeResourcePath(targetScope, types.TargetResource("res://"+strings.TrimPrefix(r.URL.Path, "/")))
		if targetResource == "" {
			return nil, httpx.ErrApplicationError("unable to canonicalize resource path")
		}

		resourceName := getResourceNameFromPath(r)
		// For resources, policies are applied to the resource path
		if resourceName == types.ResourceNameResources {
			targetResource = types.TargetResource(strings.TrimSuffix(string(targetResource), "/definition"))
		}

		// Validate against the policy
		allowed := false
		for _, action := range handler.AllowedActions {
			if authorizedViewDef.Rules.IsActionAllowed(action, targetResource) {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, ErrBlockedByPolicy
		}

		// If we get here, we are good to go, so call the handler
		return handler.Handler(r)
	}
}
