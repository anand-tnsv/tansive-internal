package catalogmanager

import (
	"fmt"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/pkg/types"
)

// validateViewRuleIntent checks if the effect is one of the allowed values.
func validateViewRuleIntent(fl validator.FieldLevel) bool {
	effect := types.Intent(fl.Field().String())
	return effect == types.IntentAllow || effect == types.IntentDeny
}

// validateViewRuleAction checks if the action is one of the allowed values.
func validateViewRuleAction(fl validator.FieldLevel) bool {
	action := types.Action(fl.Field().String())
	return slices.Contains(types.ValidActions, action)
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
			parts[0] = strings.TrimPrefix(parts[0], separator)
			parts[0] = strings.TrimPrefix(parts[0], "/")
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
		types.ResourceNameCatalogs:   0,
		types.ResourceNameVariants:   1,
		types.ResourceNameWorkspaces: 2,
		types.ResourceNameNamespaces: -1, // Special case, handled below
	}

	for key, pos := range found {
		expectedPos, exists := expectedOrder[key]
		if !exists {
			return fmt.Errorf("invalid path: unknown key '%s'", key)
		}

		if key == types.ResourceNameNamespaces {
			if _, hasWorkspace := found[types.ResourceNameWorkspaces]; hasWorkspace {
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
	v := schemavalidator.V()
	v.RegisterValidation("viewRuleIntentValidator", validateViewRuleIntent)
	v.RegisterValidation("viewRuleActionValidator", validateViewRuleAction)
	v.RegisterValidation("resourceURIValidator", validateResourceURI)
}
