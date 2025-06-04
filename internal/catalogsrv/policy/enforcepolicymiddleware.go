package policy

import (
	"context"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func EnforceViewPolicyMiddleware(handler ResponseHandlerParam) httpx.RequestHandler {
	return func(r *http.Request) (*httpx.Response, error) {
		ctx := r.Context()

		// Resolve the authorized view definition
		authorizedViewDef, err := resolveAuthorizedViewDef(ctx)
		if err != nil {
			return nil, err
		}

		// Resolve the target scope
		targetScope, err := resolveTargetScope(ctx)
		if err != nil {
			return nil, err
		}

		// Resolve the target resource
		targetResource, err := resolveTargetResource(targetScope, r.URL.Path)
		if err != nil {
			return nil, err
		}

		// Validate against the policy
		allowed := false
		matchedRules := map[Intent][]Rule{
			IntentAllow: {},
			IntentDeny:  {},
		}
		for _, action := range handler.AllowedActions {
			isAllowed, ruleSet := authorizedViewDef.Rules.IsActionAllowed(action, targetResource)

			// Track rules
			for intent, rules := range ruleSet {
				matchedRules[intent] = append(matchedRules[intent], rules...)
			}

			if isAllowed {
				allowed = true
				break
			}
		}

		// log the policy decision
		logger := log.Ctx(ctx).With().
			Str("event_type", "policy_decision").
			Str("target_resource", string(targetResource)).
			Interface("handler_actions", handler.AllowedActions).
			Bool("allowed", allowed).
			Interface("matched_allow_rules", matchedRules[IntentAllow]).
			Interface("matched_deny_rules", matchedRules[IntentDeny]).
			Logger()

		if !allowed {
			logger.Warn().Msg("access denied")
			return nil, ErrDisallowedByPolicy
		}
		logger.Info().Msg("access allowed")

		// If we get here, we are good to go, so call the handler
		return handler.Handler(r)
	}
}

func getResourceKindFromPath(resourcePath string) string {
	path := strings.Trim(resourcePath, "/")
	segments := strings.Split(path, "/")
	var resourceKind string
	if len(segments) > 0 {
		resourceKind = segments[0]
	}
	return resourceKind
}

func normalizeResourcePath(resourceKind string, resource TargetResource) TargetResource {
	if resourceKind == catcommon.KindNameResources {
		const prefix = "/resources/definition"
		if strings.HasPrefix(string(resource), prefix) {
			// Rewrite /resources/definition/... → /resources/...
			return TargetResource("/resources" + strings.TrimPrefix(string(resource), prefix))
		}
	}
	return resource
}

func resolveTargetResource(scope Scope, resourcePath string) (TargetResource, error) {
	targetResource := TargetResource(resourcePath)
	targetResource = normalizeResourcePath(getResourceKindFromPath(resourcePath), targetResource)
	targetResource = CanonicalizeResourcePath(scope, TargetResource("res://"+strings.TrimPrefix(string(targetResource), "/")))
	if targetResource == "" {
		return "", httpx.ErrApplicationError("unable to canonicalize resource path")
	}
	return targetResource, nil
}

func resolveAuthorizedViewDef(ctx context.Context) (*ViewDefinition, error) {
	c := catcommon.GetCatalogContext(ctx)
	if c == nil {
		return nil, httpx.ErrUnAuthorized("missing request context")
	}
	// Get the authorized view definition from the context
	authorizedViewDef := CanonicalizeViewDefinition(GetViewDefinition(ctx))
	if authorizedViewDef == nil {
		return nil, httpx.ErrUnAuthorized("unable to resolve view definition")
	}
	return authorizedViewDef, nil
}

func resolveTargetScope(ctx context.Context) (Scope, error) {
	c := catcommon.GetCatalogContext(ctx)
	if c == nil {
		return Scope{}, httpx.ErrUnAuthorized("missing request context")
	}
	return Scope{
		Catalog:   c.Catalog,
		Variant:   c.Variant,
		Namespace: c.Namespace,
	}, nil
}
