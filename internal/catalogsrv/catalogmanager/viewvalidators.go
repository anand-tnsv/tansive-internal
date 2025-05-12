package catalogmanager

import (
	"errors"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
)

// viewRuleEffectValidator validates that the effect is one of the allowed values.
func viewRuleEffectValidator(fl validator.FieldLevel) bool {
	effect := ViewRuleEffect(fl.Field().String())
	return effect == ViewRuleEffectAllow || effect == ViewRuleEffectDeny
}

// viewRuleActionValidator validates that the action is one of the allowed values.
func viewRuleActionValidator(fl validator.FieldLevel) bool {
	action := ViewRuleAction(fl.Field().String())
	return slices.Contains(validActions, action)
}

func init() {

}

// resourceURIValidator validates resource URIs with the following structure:
// - Must start with "res://"
// - Must have a location component that follows the hierarchy:
//   * catalog/<name> (required)
//   * /variant/<name> (optional, requires catalog)
//   * /namespace/<name> (optional, requires variant)
//   * /workspace/<name> (optional, requires variant)
// - May have additional path segments after the location
// - All names must:
//   * Start and end with alphanumeric
//   * Contain only alphanumeric and hyphens
//   * Follow DNS label rules (max 63 chars, no uppercase)
//
// Examples:
//   res://catalog/my-catalog
//   res://catalog/my-catalog/variant/my-variant
//   res://catalog/my-catalog/variant/my-variant/namespace/my-namespace
//   res://catalog/my-catalog/variant/my-variant/workspace/my-workspace
//   res://catalog/my-catalog/variant/my-variant/resource/path
//   res://catalog/my-catalog/variant/my-variant/resource/path/*

func resourceURIValidator(fl validator.FieldLevel) bool {
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
	const prefix = "res://"
	if !strings.HasPrefix(s, prefix) {
		return nil, errors.New("invalid resource string: missing res:// prefix")
	}

	rest := strings.TrimPrefix(s, prefix)
	parts := strings.SplitN(rest, "/resource/", 2)
	segments := []string{}

	segments = append(segments, parts[0])

	if len(parts) == 2 {
		if parts[1] != "" {
			parts[1] = "/" + parts[1]
		}
		segments = append(segments, parts[1])
	}

	return segments, nil
}

func isValidStructuredPath(path string) error {
	if path == "" {
		return errors.New("invalid path: empty")
	}
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments)%2 != 0 {
		return errors.New("invalid path: missing kv pair")
	}
	found := make(map[string]int)
	pos := 0
	for i := 0; i < len(segments) && i+1 < len(segments); i++ {
		key := segments[i]
		value := segments[i+1]
		if key == "" || value == "" {
			return errors.New("invalid path: missing key or value")
		}
		if _, ok := found[key]; ok {
			return errors.New("invalid path: duplicate key")
		}
		if value != "*" && schemavalidator.V().Var(value, "resourceNameValidator") != nil {
			return errors.New("invalid path: invalid value")
		}
		found[key] = pos
		pos++
		i++
	}
	for key, pos := range found {
		switch key {
		case "catalog":
			if pos != 0 {
				return errors.New("catalog must be the first segment")
			}
		case "variant":
			if pos != 1 {
				return errors.New("variant must be the second segment")
			}
		case "workspace":
			if pos != 2 {
				return errors.New("workspace must be the third segment")
			}
		case "namespace":
			if _, ok := found["workspace"]; !ok {
				if pos != 2 {
					return errors.New("namespace must be the third segment")
				}
			} else {
				if pos != 3 {
					return errors.New("namespace must be the fourth segment")
				}
			}
		default:
			return errors.New("invalid path: unknown key")
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

func (r ViewRuleSet) matchesAdmin(inputAction ViewRuleAction, resource string) bool {
	if resource == "" {
		return false
	}
	resourceSegments, err := extractSegments(resource)
	if err != nil {
		return false
	}
	if len(resourceSegments) == 0 {
		return false
	}
	resourceMetadata, err := extractMetadata(resourceSegments[0])
	if err != nil {
		return false
	}
	for _, rule := range r {
		if rule.Effect == ViewRuleEffectAllow {
			var adminActions = make(map[ViewRuleAction]bool)
			for _, action := range rule.Action {
				switch action {
				case ActionCatalogAdmin:
					adminActions[action] = true
				case ActionVariantAdmin:
					adminActions[action] = true
				case ActionNamespaceAdmin:
					adminActions[action] = true
				case ActionWorkspaceAdmin:
					adminActions[action] = true
				}
			}
			if len(adminActions) == 0 {
				return false
			}
			// first get the matching rules from ruleset
			for _, res := range rule.Resource {
				segments, err := extractSegments(string(res))
				if err != nil {
					continue
				}
				if len(segments) != 1 {
					continue
				}
				m, err := extractMetadata(segments[0])
				if err != nil {
					continue
				}
				// validation must be in order
				if adminActions[ActionCatalogAdmin] {
					if m["catalog"].pos == resourceMetadata["catalog"].pos && (m["catalog"].value == "*" || (m["catalog"].value == resourceMetadata["catalog"].value)) {
						if len(m) == 1 && matchParentResource("catalog", m, resourceMetadata) {
							return true
						}
					}
				}
				if adminActions[ActionVariantAdmin] {
					if m["variant"].pos == resourceMetadata["variant"].pos && (m["variant"].value == "*" || (m["variant"].value == resourceMetadata["variant"].value)) {
						if len(m) == 2 && matchParentResource("variant", m, resourceMetadata) {
							return true
						}
					}
				}
				if adminActions[ActionWorkspaceAdmin] {
					if m["workspace"].pos == resourceMetadata["workspace"].pos && (m["workspace"].value == "*" || (m["workspace"].value == resourceMetadata["workspace"].value)) {
						if len(m) == 3 && matchParentResource("workspace", m, resourceMetadata) {
							return true
						}
					}
				}
				if adminActions[ActionNamespaceAdmin] {
					if m["namespace"].pos == resourceMetadata["namespace"].pos && (m["namespace"].value == "*" || (m["namespace"].value == resourceMetadata["namespace"].value)) {
						if _, ok := m["workspace"]; !ok {
							if len(m) == 3 && matchParentResource("namespace", m, resourceMetadata) {
								return true
							}
						} else {
							if len(m) == 4 && matchParentResource("namespace", m, resourceMetadata) {
								return true
							}
						}
					}
				}
			}

		}
	}

	return false
}

func (r RuleResource) matches(actualRes RuleResource) bool {
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
	validate.RegisterValidation("viewRuleEffectValidator", viewRuleEffectValidator)
	validate.RegisterValidation("viewRuleActionValidator", viewRuleActionValidator)
	validate.RegisterValidation("resourceURIValidator", resourceURIValidator)
}
