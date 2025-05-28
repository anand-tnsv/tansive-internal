package apis

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
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

// resourceObjectHandlers defines the API routes and their authorization requirements.
// Each route requires at least one of the listed actions to be authorized.
var resourceObjectHandlers = []httpx.ResponseHandlerParam{
	{
		Method:         http.MethodGet,
		Path:           "/catalogs/{catalogName}",
		Handler:        getObject,
		AllowedActions: []policy.Action{policy.ActionCatalogList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/catalogs/{catalogName}",
		Handler:        updateObject,
		AllowedActions: []policy.Action{policy.ActionCatalogAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/catalogs/{catalogName}",
		Handler:        deleteObject,
		AllowedActions: []policy.Action{policy.ActionCatalogAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/variants",
		Handler:        createObject,
		AllowedActions: []policy.Action{policy.ActionVariantClone},
	},
	{
		Method:         http.MethodGet,
		Path:           "/variants/{variantName}",
		Handler:        getObject,
		AllowedActions: []policy.Action{policy.ActionVariantList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/variants/{variantName}",
		Handler:        updateObject,
		AllowedActions: []policy.Action{policy.ActionVariantAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/variants/{variantName}",
		Handler:        deleteObject,
		AllowedActions: []policy.Action{policy.ActionVariantAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/namespaces",
		Handler:        createObject,
		AllowedActions: []policy.Action{policy.ActionNamespaceCreate},
	},
	{
		Method:         http.MethodGet,
		Path:           "/namespaces/{namespaceName}",
		Handler:        getObject,
		AllowedActions: []policy.Action{policy.ActionNamespaceList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/namespaces/{namespaceName}",
		Handler:        updateObject,
		AllowedActions: []policy.Action{policy.ActionNamespaceAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/namespaces/{namespaceName}",
		Handler:        deleteObject,
		AllowedActions: []policy.Action{policy.ActionNamespaceAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/views",
		Handler:        createObject,
		AllowedActions: []policy.Action{policy.ActionCatalogCreateView},
	},
	{
		Method:         http.MethodGet,
		Path:           "/views/{viewName}",
		Handler:        getObject,
		AllowedActions: []policy.Action{policy.ActionCatalogList},
	},
	{
		Method:         http.MethodPut,
		Path:           "/views/{viewName}",
		Handler:        updateObject,
		AllowedActions: []policy.Action{policy.ActionViewAdmin},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/views/{viewName}",
		Handler:        deleteObject,
		AllowedActions: []policy.Action{policy.ActionViewAdmin},
	},
	{
		Method:         http.MethodPost,
		Path:           "/resources",
		Handler:        createObject,
		AllowedActions: []policy.Action{policy.ActionResourceCreate},
	},
	{
		Method:         http.MethodGet,
		Path:           "/resources",
		Handler:        listObjects,
		AllowedActions: []policy.Action{policy.ActionResourceList},
	},
	{
		Method:         http.MethodGet,
		Path:           "/resources/{resourcePath:.+}/definition",
		Handler:        getObject,
		AllowedActions: []policy.Action{policy.ActionResourceRead, policy.ActionResourceEdit},
	},
	{
		Method:         http.MethodGet,
		Path:           "/resources/{resourceValue:.+}",
		Handler:        getObject,
		AllowedActions: []policy.Action{policy.ActionResourceGet, policy.ActionResourcePut},
	},
	{
		Method:         http.MethodPut,
		Path:           "/resources/{resourceValue:.+}",
		Handler:        updateObject,
		AllowedActions: []policy.Action{policy.ActionResourcePut},
	},
	{
		Method:         http.MethodPut,
		Path:           "/resources/{resourcePath:.+}/definition",
		Handler:        updateObject,
		AllowedActions: []policy.Action{policy.ActionResourceEdit},
	},
	{
		Method:         http.MethodDelete,
		Path:           "/resources/{resourcePath:.+}/definition",
		Handler:        deleteObject,
		AllowedActions: []policy.Action{policy.ActionResourceDelete},
	},
}

// Router creates and configures a new router for catalog service API endpoints.
// It sets up middleware and registers handlers for various HTTP methods and paths.
func Router(r chi.Router) chi.Router {
	router := chi.NewRouter()
	//Load the group that needs only user session/identity validation
	router.Group(func(r chi.Router) {
		r.Use(auth.UserAuthMiddleware)
		r.Use(LoadCatalogContext)
		for _, handler := range userSessionHandlers {
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
		}
	})

	//Load the group that needs session validation and catalog context
	router.Group(func(r chi.Router) {
		r.Use(auth.ContextMiddleware)
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
		tenantID := catcommon.GetTenantID(ctx)
		projectID := catcommon.GetProjectID(ctx)
		if tenantID == "" || projectID == "" {
			httpx.ErrInvalidRequest().Send(w)
			return
		}
		c := catcommon.GetCatalogContext(ctx)
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
		ctx = catcommon.WithCatalogContext(ctx, c)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// EnforceViewPolicy is a middleware that enforces view-based access control policies.
// It validates that the request is allowed based on the view definition and policy rules.
func EnforceViewPolicy(handler httpx.ResponseHandlerParam) httpx.RequestHandler {
	return func(r *http.Request) (*httpx.Response, error) {
		ctx := r.Context()

		//Get allowed actions from Context. This is set from the token by validation middleware
		c := catcommon.GetCatalogContext(ctx)
		if c == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}
		authorizedViewDef := policy.CanonicalizeViewDefinition(auth.GetViewDefinition(ctx))
		if authorizedViewDef == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}

		// Build the metadata path
		targetScope := policy.Scope{
			Catalog:   c.Catalog,
			Variant:   c.Variant,
			Namespace: c.Namespace,
		}
		targetResource := policy.CanonicalizeResourcePath(targetScope, policy.TargetResource("res://"+strings.TrimPrefix(r.URL.Path, "/")))
		if targetResource == "" {
			return nil, httpx.ErrApplicationError("unable to canonicalize resource path")
		}

		resourceName := getResourceNameFromPath(r)
		// For resources, policies are applied to the resource path
		if resourceName == catcommon.ResourceNameResources {
			targetResource = policy.TargetResource(strings.TrimSuffix(string(targetResource), "/definition"))
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
