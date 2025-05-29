package policy

import (
	"context"
	"slices"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

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

// ValidateDerivedView ensures that a derived view is valid with respect to its parent view.
// It ensures that the derived view's scope is the same as the parent's and that all rules in the derived view
// are permissible by the parent view.
func ValidateDerivedView(ctx context.Context, parent *ViewDefinition, child *ViewDefinition) apperrors.Error {
	if parent == nil || child == nil {
		return ErrInvalidView
	}

	parent = CanonicalizeViewDefinition(parent)
	child = CanonicalizeViewDefinition(child)

	if !child.Rules.IsSubsetOf(parent.Rules) {
		return ErrInvalidView.New("derived view rules must be a subset of parent view rules")
	}

	return nil
}
