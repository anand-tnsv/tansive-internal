package policy

import (
	"context"
	"slices"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
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

	parent = CanonicalizeViewDefinition(parent)
	child = CanonicalizeViewDefinition(child)

	if !child.Rules.IsSubsetOf(parent.Rules) {
		return ErrInvalidView.New("derived view rules must be a subset of parent view rules")
	}

	return nil
}

func AreActionsAllowed(ctx context.Context, vd *ViewDefinition, actions []Action) bool {
	if vd == nil {
		return false
	}
	for _, action := range actions {
		allowed := false
		for _, rule := range vd.Rules {
			if slices.Contains(rule.Actions, action) {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	return true
}
