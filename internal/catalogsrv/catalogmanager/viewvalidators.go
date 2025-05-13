package catalogmanager

import (
	"fmt"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/pkg/types"
)

// Constants for resource types
const (
	resourceTypeCatalog   = "catalog"
	resourceTypeVariant   = "variant"
	resourceTypeWorkspace = "workspace"
	resourceTypeNamespace = "namespace"
)

// adminActionMap represents a set of admin actions
type adminActionMap map[Action]bool

// buildAdminActionMap creates a map of admin actions from a slice of actions
func buildAdminActionMap(actions []Action) adminActionMap {
	adminActions := make(adminActionMap)
	for _, action := range actions {
		switch action {
		case ActionCatalogAdmin, ActionVariantAdmin, ActionNamespaceAdmin, ActionWorkspaceAdmin:
			adminActions[action] = true
		}
	}
	return adminActions
}

// validateViewRuleIntent checks if the effect is one of the allowed values.
func validateViewRuleIntent(fl validator.FieldLevel) bool {
	effect := Intent(fl.Field().String())
	return effect == IntentAllow || effect == IntentDeny
}

// validateViewRuleAction checks if the action is one of the allowed values.
func validateViewRuleAction(fl validator.FieldLevel) bool {
	action := Action(fl.Field().String())
	return slices.Contains(validActions, action)
}

// validateResourceURI checks if the resource URI follows the required structure.
func validateResourceURI(fl validator.FieldLevel) bool {
	uri := fl.Field().String()
	segments, err := extractSegments(uri)
	if err != nil {
		return false
	}

	if len(segments) == 0 || isValidStructuredPath(segments[0]) != nil {
		return false
	}

	if len(segments) > 1 {
		prefix, hasWildcard := extractPrefixIfWildcard(segments[1])
		if hasWildcard && prefix == "" {
			return true
		}
		return schemavalidator.V().Var(prefix, "resourcePathValidator") == nil
	}

	return true
}

func extractSegments(s string) ([]string, error) {
	segments, _, err := extractSegmentsAndResourceName(s)
	return segments, err
}

func extractSegmentsAndResourceName(s string) ([]string, string, error) {
	const prefix = "res://"
	if !strings.HasPrefix(s, prefix) {
		return nil, "", fmt.Errorf("invalid resource string: missing %s prefix", prefix)
	}

	separators := types.ResourceURIs()

	rest := strings.TrimPrefix(s, prefix)
	var parts = []string{rest}
	resourceName := ""
	for _, separator := range separators {
		parts = strings.SplitN(rest, "/"+separator+"/", 2)
		if len(parts) == 2 {
			resourceName = separator
			break
		}
		if strings.HasPrefix(parts[0], separator) {
			parts = []string{"", parts[0]}
			resourceName = separator
			break
		}
	}

	segments := []string{}

	segments = append(segments, parts[0])

	if len(parts) == 2 {
		if parts[1] != "" {
			parts[1] = "/" + parts[1]
		}
		segments = append(segments, parts[1])
	}

	return segments, resourceName, nil

}

func isValidStructuredPath(path string) error {
	if path == "" {
		return fmt.Errorf("invalid path: empty")
	}

	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments)%2 != 0 {
		return fmt.Errorf("invalid path: missing key-value pair")
	}

	found := make(map[string]int)
	for i := 0; i < len(segments)-1; i += 2 {
		key := segments[i]
		value := segments[i+1]

		if key == "" || value == "" {
			return fmt.Errorf("invalid path: empty key or value at position %d", i)
		}

		if _, ok := found[key]; ok {
			return fmt.Errorf("invalid path: duplicate key '%s'", key)
		}

		if value != "*" && schemavalidator.V().Var(value, "resourceNameValidator") != nil {
			return fmt.Errorf("invalid path: invalid value '%s' for key '%s'", value, key)
		}

		found[key] = i / 2
	}

	// Validate segment order
	expectedOrder := map[string]int{
		resourceTypeCatalog:   0,
		resourceTypeVariant:   1,
		resourceTypeWorkspace: 2,
		resourceTypeNamespace: -1, // Special case, handled below
	}

	for key, pos := range found {
		expectedPos, exists := expectedOrder[key]
		if !exists {
			return fmt.Errorf("invalid path: unknown key '%s'", key)
		}

		if key == resourceTypeNamespace {
			if _, hasWorkspace := found[resourceTypeWorkspace]; hasWorkspace {
				expectedPos = 3
			} else {
				expectedPos = 2
			}
		}

		if pos != expectedPos {
			return fmt.Errorf("invalid path: '%s' must be at position %d, found at %d", key, expectedPos, pos)
		}
	}

	return nil
}

func extractPrefixIfWildcard(path string) (string, bool) {
	const suffix = "/*"
	if strings.HasSuffix(path, suffix) {
		prefix := strings.TrimSuffix(path, suffix)
		return prefix, true
	}
	return path, false
}

func checkAdminMatch(resourceType string, ruleSegments []string) bool {
	lenRule := len(ruleSegments)
	if lenRule < 2 {
		return false
	}
	if ruleSegments[lenRule-2] == resourceType {
		return true
	}
	return false
}

func (r ViewRuleSet) matchesAdmin(resource string) bool {
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
			if adminActions[ActionCatalogAdmin] && checkAdminMatch(resourceTypeCatalog, ruleSegments) {
				isMatch = true
			}
			if adminActions[ActionVariantAdmin] && checkAdminMatch(resourceTypeVariant, ruleSegments) {
				isMatch = true
			}
			if adminActions[ActionWorkspaceAdmin] && checkAdminMatch(resourceTypeWorkspace, ruleSegments) {
				isMatch = true
			}
			if adminActions[ActionNamespaceAdmin] && checkAdminMatch(resourceTypeNamespace, ruleSegments) {
				isMatch = true
			}
			if isMatch && res.matches(resource) {
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
		if actualSegments[i] == "*" {
			return false
		}
		if ruleSegments[i] == "*" || ruleSegments[i] == actualSegments[i] {
			continue
		}
		return false
	}

	return true
}

type resourceMetadataValue struct {
	value string
	pos   int
}

func extractKV(m string) map[string]resourceMetadataValue {
	segments := strings.Split(strings.Trim(m, "/"), "/")
	metadata := make(map[string]resourceMetadataValue)
	pos := 0
	segmentLen := len(segments)
	for i := 0; i < segmentLen; i++ {
		key := segments[i]
		value := ""
		i++
		if i < segmentLen {
			value = segments[i]
		}
		metadata[key] = resourceMetadataValue{value: value, pos: pos}
		pos++
	}
	return metadata
}

func init() {
	validate := schemavalidator.V()
	validate.RegisterValidation("viewRuleIntentValidator", validateViewRuleIntent)
	validate.RegisterValidation("viewRuleActionValidator", validateViewRuleAction)
	validate.RegisterValidation("resourceURIValidator", validateResourceURI)
}
