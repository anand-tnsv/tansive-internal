package policy

import (
	"net/http"

	"github.com/rs/zerolog/log"
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
			isAllowed, ruleSet := authorizedViewDef.Rules.IsActionAllowedOnResource(action, targetResource)

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
