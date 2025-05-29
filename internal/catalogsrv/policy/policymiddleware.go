package policy

import (
	"net/http"
	"strings"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func EnforceViewPolicy(handler ResponseHandlerParam) httpx.RequestHandler {
	return func(r *http.Request) (*httpx.Response, error) {
		ctx := r.Context()

		//Get allowed actions from Context. This is set from the token by validation middleware
		c := catcommon.GetCatalogContext(ctx)
		if c == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}
		authorizedViewDef := CanonicalizeViewDefinition(GetViewDefinition(ctx))
		if authorizedViewDef == nil {
			return nil, httpx.ErrUnAuthorized("missing or invalid authorization token")
		}

		// Build the metadata path
		targetScope := Scope{
			Catalog:   c.Catalog,
			Variant:   c.Variant,
			Namespace: c.Namespace,
		}
		targetResource := CanonicalizeResourcePath(targetScope, TargetResource("res://"+strings.TrimPrefix(r.URL.Path, "/")))
		if targetResource == "" {
			return nil, httpx.ErrApplicationError("unable to canonicalize resource path")
		}

		resourceName := getResourceNameFromPath(r)
		// For resources, policies are applied to the resource path
		if resourceName == catcommon.KindNameResources {
			targetResource = TargetResource(strings.TrimSuffix(string(targetResource), "/definition"))
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
			return nil, ErrDisallowedByPolicy
		}

		// If we get here, we are good to go, so call the handler
		return handler.Handler(r)
	}
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
