package policy

import (
	"context"
	"slices"
	"strings"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

// IsActionAllowedOnResource checks if a given action is allowed for a specific resource based on the rule set.
// It returns true if the action is allowed, false otherwise. Deny rules take precedence over allow rules.
func (ruleSet Rules) IsActionAllowedOnResource(action Action, target TargetResource) (bool, map[Intent][]Rule) {
	matchedRulesAllow := []Rule{}
	matchedRulesDeny := []Rule{}
	allowMatch := false
	// check if there is an admin match
	allowMatch, matchedRule := ruleSet.matchesAdmin(string(target))
	if allowMatch {
		allowMatch = true
		matchedRulesAllow = append(matchedRulesAllow, matchedRule)
	}
	// check if there is a match for the action
	for _, rule := range ruleSet {
		if slices.Contains(rule.Actions, action) {
			for _, res := range rule.Targets {
				if rule.Intent == IntentAllow {
					if res.matches(string(target)) {
						allowMatch = true
						matchedRulesAllow = append(matchedRulesAllow, rule)
					}
				} else if rule.Intent == IntentDeny {
					if res.matches(string(target)) || // target is allowed by the rule
						target.matches(string(res)) { // target is more permissive than the rule when we evaluate rule subsets
						allowMatch = false
						matchedRulesDeny = append(matchedRulesDeny, rule)
					}
				}
			}
		}
	}
	return allowMatch, map[Intent][]Rule{
		IntentAllow: matchedRulesAllow,
		IntentDeny:  matchedRulesDeny,
	}
}

// IsSubsetOf checks if this ViewRuleSet is a subset of another ViewRuleSet.
// Returns true if every action and target in this set is permissible by the other set.
func (ruleSet Rules) IsSubsetOf(other Rules) bool {
	for _, rule := range ruleSet {
		for _, action := range rule.Actions {
			for _, target := range rule.Targets {
				if rule.Intent == IntentAllow {
					allow, _ := other.IsActionAllowedOnResource(action, target)
					if !allow {
						return false
					}
				}
			}
		}
	}
	return true
}

// ValidateDerivedView ensures that a derived view is valid with respect to its parent view.
// It ensures that the derived view's scope is the same as the parent's and that all rules in the derived view
// are permissible by the parent view.
func ValidateDerivedView(ctx context.Context, parent *ViewDefinition, child *ViewDefinition) apperrors.Error {
	if parent == nil || child == nil {
		return ErrInvalidView
	}

	parent = canonicalizeViewDefinition(parent)
	child = canonicalizeViewDefinition(child)

	if !child.Rules.IsSubsetOf(parent.Rules) {
		return ErrInvalidView.New("derived view rules must be a subset of parent view rules")
	}

	return nil
}

func AreActionsAllowedOnResource(ctx context.Context, vd *ViewDefinition, resource string, actions []Action) (bool, apperrors.Error) {
	if vd == nil {
		return false, ErrInvalidView.Msg("view definition is nil")
	}
	if resource == "" {
		return false, ErrInvalidView.Msg("resource is empty")
	}
	if len(actions) == 0 {
		return false, ErrInvalidView.Msg("actions are empty")
	}

	scope, err := resolveTargetScope(ctx)
	if err != nil {
		return false, ErrInvalidView.New(err.Error())
	}
	targetResource, err := resolveTargetResource(scope, resource)
	if err != nil {
		return false, ErrInvalidView.New(err.Error())
	}

	vd = canonicalizeViewDefinition(vd)

	for _, action := range actions {
		allowed, _ := vd.Rules.IsActionAllowedOnResource(action, targetResource)
		if !allowed {
			return false, nil
		}
	}
	return true, nil
}

func CanAdoptView(ctx context.Context, view string) (bool, apperrors.Error) {
	catalog := catcommon.GetCatalog(ctx)
	if catalog == "" {
		return false, ErrInvalidView.Msg("unable to resolve catalog")
	}
	viewResource, _ := resolveTargetResource(Scope{Catalog: catalog}, "/views/"+view)
	ourViewDef, err := resolveAuthorizedViewDef(ctx)
	if err != nil {
		return false, ErrInvalidView.New(err.Error())
	}
	if ourViewDef == nil {
		return false, ErrInvalidView.Msg("unable to resolve view definition")
	}
	allowed, _ := ourViewDef.Rules.IsActionAllowedOnResource(ActionCatalogAdoptView, viewResource)
	return allowed, nil
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
	resourcePath = strings.TrimPrefix(resourcePath, "res://")
	targetResource := TargetResource(resourcePath)
	targetResource = normalizeResourcePath(getResourceKindFromPath(resourcePath), targetResource)
	targetResource = canonicalizeResourcePath(scope, TargetResource("res://"+strings.TrimPrefix(string(targetResource), "/")))
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
	authorizedViewDef := canonicalizeViewDefinition(GetViewDefinition(ctx))
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
