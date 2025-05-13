package catalogmanager

import (
	"errors"
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
	separators = append(separators, "resource")

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

// validateResourceSegments validates the basic structure of resource segments
func validateResourceSegments(resource string) (map[string]resourceMetadataValue, error) {
	if resource == "" {
		return nil, fmt.Errorf("empty resource")
	}

	segments, err := extractSegments(resource)
	if err != nil || len(segments) == 0 {
		return nil, fmt.Errorf("invalid segments: %v", err)
	}

	metadata, err := extractMetadata(segments[0])
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %v", err)
	}

	return metadata, nil
}

// checkAdminMatch checks if the admin rule matches for a specific resource type
func checkAdminMatch(resourceType string, m, resourceMetadata map[string]resourceMetadataValue) bool {
	metaValue, exists := m[resourceType]
	if !exists {
		return false
	}

	resourceValue, exists := resourceMetadata[resourceType]
	if !exists {
		return false
	}

	if metaValue.pos != resourceValue.pos {
		return false
	}

	if metaValue.value != "*" && metaValue.value != resourceValue.value {
		return false
	}

	expectedLen := map[string]int{
		resourceTypeCatalog:   1,
		resourceTypeVariant:   2,
		resourceTypeWorkspace: 3,
		resourceTypeNamespace: 3, // or 4 with workspace
	}

	if resourceType == resourceTypeNamespace {
		if _, hasWorkspace := m["workspace"]; hasWorkspace {
			expectedLen[resourceTypeNamespace] = 4
		}
	}

	return len(m) == expectedLen[resourceType] && matchParentResource(resourceType, m, resourceMetadata)
}

func (r ViewRuleSet) matchesAdmin(resource string) bool {
	metadata, err := validateResourceSegments(resource)
	if err != nil {
		return false
	}

	for _, rule := range r {
		if rule.Intent != IntentAllow {
			continue
		}

		adminActions := buildAdminActionMap(rule.Actions)
		if len(adminActions) == 0 {
			continue
		}

		for _, res := range rule.Targets {
			ruleSegments, err := extractSegments(string(res))
			if err != nil || len(ruleSegments) != 1 {
				continue
			}

			m, err := extractMetadata(ruleSegments[0])
			if err != nil {
				continue
			}

			// Check each admin action type
			if adminActions[ActionCatalogAdmin] && checkAdminMatch(resourceTypeCatalog, m, metadata) {
				return true
			}
			if adminActions[ActionVariantAdmin] && checkAdminMatch(resourceTypeVariant, m, metadata) {
				return true
			}
			if adminActions[ActionWorkspaceAdmin] && checkAdminMatch(resourceTypeWorkspace, m, metadata) {
				return true
			}
			if adminActions[ActionNamespaceAdmin] && checkAdminMatch(resourceTypeNamespace, m, metadata) {
				return true
			}
		}
	}
	return false
}

func (r TargetResource) matches(actualRes TargetResource) bool {
	ruleSegments, err := extractSegments(string(r))
	if err != nil {
		return false
	}
	actualSegments, err := extractSegments(string(actualRes))
	if err != nil {
		return false
	}
	if len(actualSegments) > 0 && len(ruleSegments) > 0 {
		if !matchesMetadata(ruleSegments[0], actualSegments[0]) {
			return false
		}
	}
	if len(actualSegments) > 1 {
		if len(ruleSegments) > 1 {
			if !matchesResource(ruleSegments[1], actualSegments[1]) {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

func matchesMetadata(ruleMeta, actualMeta string) bool {
	ruleMetaSegments, err := extractMetadata(ruleMeta)
	if err != nil {
		return false
	}
	actualMetaSegments, err := extractMetadata(actualMeta)
	if err != nil {
		return false
	}
	for key, value := range actualMetaSegments {
		if val, ok := ruleMetaSegments[key]; !ok || (val.value != "*" && val.value != value.value) || (val.pos != value.pos) {
			return false
		}
	}
	return true
}

type resourceMetadataValue struct {
	value string
	pos   int
}

func extractMetadata(m string) (map[string]resourceMetadataValue, error) {
	segments := strings.Split(strings.Trim(m, "/"), "/")
	metadata := make(map[string]resourceMetadataValue)
	if len(segments)%2 != 0 {
		return nil, errors.New("invalid metadata: missing key or value")
	}
	pos := 0
	for i := 0; i < len(segments) && i+1 < len(segments); i++ {
		key := segments[i]
		value := segments[i+1]
		if key == "" || value == "" {
			return nil, errors.New("invalid metadata: missing key or value")
		}
		metadata[key] = resourceMetadataValue{value: value, pos: pos}
		pos++
		i++
	}
	return metadata, nil
}

// matchesResource checks if an actual resource matches a rule resource pattern.
// It supports exact matches and wildcard patterns (ending with *).
func matchesResource(ruleRes, actualRes string) bool {
	if strings.HasSuffix(ruleRes, "*") {
		prefix := strings.TrimSuffix(ruleRes, "*")
		return strings.HasPrefix(actualRes, prefix)
	}
	return ruleRes == actualRes
}

func matchParentResource(resType string, ruleRes, actualRes map[string]resourceMetadataValue) bool {
	switch resType {
	case "catalog":
		return ruleRes["catalog"].value == "*" || ruleRes["catalog"].value == actualRes["catalog"].value

	case "variant":
		return matchParentResource("catalog", ruleRes, actualRes) && (ruleRes["variant"].value == "*" || ruleRes["variant"].value == actualRes["variant"].value)

	case "workspace":
		return matchParentResource("variant", ruleRes, actualRes) && (ruleRes["workspace"].value == "*" || ruleRes["workspace"].value == actualRes["workspace"].value)

	case "namespace":
		if _, ok := actualRes["workspace"]; !ok {
			return matchParentResource("variant", ruleRes, actualRes) && (ruleRes["namespace"].value == "*" || ruleRes["namespace"].value == actualRes["namespace"].value)
		} else {
			return matchParentResource("workspace", ruleRes, actualRes) && (ruleRes["namespace"].value == "*" || ruleRes["namespace"].value == actualRes["namespace"].value)
		}
	}
	return false
}

func init() {
	validate := schemavalidator.V()
	validate.RegisterValidation("viewRuleIntentValidator", validateViewRuleIntent)
	validate.RegisterValidation("viewRuleActionValidator", validateViewRuleAction)
	validate.RegisterValidation("resourceURIValidator", validateResourceURI)
}
