package policy

import (
	json "github.com/json-iterator/go"
	"slices"
	"strings"
)

type Intent string

const (
	IntentAllow Intent = "Allow"
	IntentDeny  Intent = "Deny"
)

type Action string

const (
	ActionCatalogAdmin      Action = "catalog.admin"
	ActionCatalogList       Action = "catalog.list"
	ActionCatalogAdoptView  Action = "catalog.adoptView"
	ActionCatalogCreateView Action = "catalog.createView"
	ActionViewAdmin         Action = "view.admin"
	ActionVariantAdmin      Action = "variant.admin"
	ActionVariantClone      Action = "variant.clone"
	ActionVariantList       Action = "variant.list"
	ActionNamespaceCreate   Action = "namespace.create"
	ActionNamespaceList     Action = "namespace.list"
	ActionNamespaceAdmin    Action = "namespace.admin"
	ActionResourceCreate    Action = "resource.create"
	ActionResourceRead      Action = "resource.read"
	ActionResourceEdit      Action = "resource.edit"
	ActionResourceDelete    Action = "resource.delete"
	ActionResourceGet       Action = "resource.get"
	ActionResourcePut       Action = "resource.put"
	ActionResourceList      Action = "resource.list"
)

var ValidActions = []Action{
	ActionCatalogAdmin,
	ActionCatalogList,
	ActionCatalogAdoptView,
	ActionCatalogCreateView,
	ActionVariantAdmin,
	ActionVariantClone,
	ActionVariantList,
	ActionNamespaceCreate,
	ActionNamespaceList,
	ActionNamespaceAdmin,
	ActionResourceCreate,
	ActionResourceRead,
	ActionResourceEdit,
	ActionResourceDelete,
	ActionResourceGet,
	ActionResourcePut,
	ActionResourceList,
}

type Rule struct {
	Intent  Intent           `json:"intent" validate:"required,viewRuleIntentValidator"`
	Actions []Action         `json:"actions" validate:"required,dive,viewRuleActionValidator"`
	Targets []TargetResource `json:"targets" validate:"-"`
}

type TargetResource string
type Rules []Rule
type Scope struct {
	Catalog   string `json:"catalog" validate:"required,resourceNameValidator"`
	Variant   string `json:"variant,omitempty" validate:"omitempty,resourceNameValidator"`
	Namespace string `json:"namespace,omitempty" validate:"omitempty,resourceNameValidator"`
}

func (v Scope) Equals(other Scope) bool {
	return v.Catalog == other.Catalog &&
		v.Variant == other.Variant &&
		v.Namespace == other.Namespace
}

type ViewDefinition struct {
	Scope Scope `json:"scope" validate:"required"`
	Rules Rules `json:"rules" validate:"required,dive"`
}

// ToJSON converts a ViewRuleSet to a JSON byte slice.
func (v ViewDefinition) ToJSON() ([]byte, error) {
	return json.Marshal(v)
}

// adminActionMap represents a set of admin actions
type adminActionMap map[Action]bool

// buildAdminActionMap creates a map of admin actions from a slice of actions
func buildAdminActionMap(actions []Action) adminActionMap {
	adminActions := make(adminActionMap)
	for _, action := range actions {
		switch action {
		case ActionCatalogAdmin, ActionVariantAdmin, ActionNamespaceAdmin:
			adminActions[action] = true
		}
	}
	return adminActions
}

// IsActionAllowed checks if a given action is allowed for a specific resource based on the rule set.
// It returns true if the action is allowed, false otherwise. Deny rules take precedence over allow rules.
func (ruleSet Rules) IsActionAllowed(action Action, target TargetResource) bool {
	allowMatch := false
	// check if there is an admin match
	if ruleSet.matchesAdmin(string(target)) {
		allowMatch = true
	}
	// check if there is a match for the action
	for _, rule := range ruleSet {
		if slices.Contains(rule.Actions, action) {
			for _, res := range rule.Targets {
				if rule.Intent == IntentAllow {
					if res.matches(string(target)) {
						allowMatch = true
					}
				} else if rule.Intent == IntentDeny {
					if res.matches(string(target)) || // target is allowed by the rule
						target.matches(string(res)) { // target is more permissive than the rule when we evaluate rule subsets
						allowMatch = false
					}
				}
			}
		}
	}
	return allowMatch
}

// IsSubsetOf checks if this ViewRuleSet is a subset of another ViewRuleSet.
// Returns true if every action and target in this set is permissible by the other set.
func (ruleSet Rules) IsSubsetOf(other Rules) bool {
	for _, rule := range ruleSet {
		for _, action := range rule.Actions {
			for _, target := range rule.Targets {
				if rule.Intent == IntentAllow && !other.IsActionAllowed(action, target) {
					return false
				}
			}
		}
	}
	return true
}

func checkAdminMatch(resourceType string, ruleSegments []string) bool {
	lenRule := len(ruleSegments)
	if lenRule < 2 {
		return false
	}
	return ruleSegments[lenRule-2] == resourceType
}

func (r Rules) matchesAdmin(resource string) bool {
	for _, rule := range r {
		if rule.Intent != IntentAllow {
			continue
		}

		adminActions := buildAdminActionMap(rule.Actions)
		if len(adminActions) == 0 {
			continue
		}

		for _, res := range rule.Targets {
			ruleSegments := strings.Split(string(res), "/")
			lenRule := len(ruleSegments)
			if lenRule < 2 {
				continue
			}
			isMatch := false
			if adminActions[ActionCatalogAdmin] && checkAdminMatch("catalogs", ruleSegments) {
				isMatch = true
			}
			if adminActions[ActionVariantAdmin] && checkAdminMatch("variants", ruleSegments) {
				isMatch = true
			}
			if adminActions[ActionNamespaceAdmin] && checkAdminMatch("namespaces", ruleSegments) {
				isMatch = true
			}
			if isMatch && (strings.HasPrefix(resource, string(res)) || res.matches(resource)) {
				return true
			}
		}
	}
	return false
}

func (r TargetResource) matches(actualRes string) bool {
	ruleSegments := strings.Split(string(r), "/")
	actualSegments := strings.Split(actualRes, "/")
	ruleLen := len(ruleSegments)
	actualLen := len(actualSegments)

	if ruleLen > actualLen {
		return false
	}

	if ruleLen < actualLen {
		if ruleSegments[ruleLen-1] != "*" {
			return false
		}
	}

	for i := 0; i < ruleLen; i++ {
		if i >= actualLen {
			return false
		}
		// if actualSegments[i] == "*" {
		// 	return false
		// }
		if ruleSegments[i] == "*" || ruleSegments[i] == actualSegments[i] {
			continue
		}
		return false
	}

	return true
}
