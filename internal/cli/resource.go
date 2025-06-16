package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// Resource represents a generic resource with Kind and metadata
type Resource struct {
	Kind     string                 `json:"kind" yaml:"kind"`
	Metadata map[string]interface{} `json:"metadata" yaml:"metadata"`
}

// LoadResourceFromFile loads a resource from a YAML file and converts it to JSON
func LoadResourceFromFile(filename string) ([]byte, *Resource, error) {
	// Read the YAML file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read file: %v", err)
	}

	// Remove stray tabs
	data = replaceTabsWithSpaces(data)

	// Convert YAML to JSON
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse YAML: %v", err)
	}

	// Parse the resource to get its kind
	var resource Resource
	if err := json.Unmarshal(jsonData, &resource); err != nil {
		return nil, nil, fmt.Errorf("failed to parse resource: %v", err)
	}

	return jsonData, &resource, nil
}

func replaceTabsWithSpaces(b []byte) []byte {
	space := []byte("    ")
	var result []byte
	for _, c := range b {
		if c == '\t' {
			result = append(result, space...)
		} else {
			result = append(result, c)
		}
	}
	return result
}

// GetResourceType returns the API endpoint path for a given resource kind
func GetResourceType(kind string) (string, error) {
	switch kind {
	case "Catalog":
		return "catalogs", nil
	case "Variant":
		return "variants", nil
	case "Namespace":
		return "namespaces", nil
	case "Workspace":
		return "workspaces", nil
	case "View":
		return "views", nil
	case "Resource":
		return "resources", nil
	case "SkillSet":
		return "skillsets", nil
	default:
		return "", fmt.Errorf("unknown resource kind: %s", kind)
	}
}

// MapResourceTypeToURL maps a resource type string to its URL format
func MapResourceTypeToURL(resourceType string) (string, error) {
	switch resourceType {
	case "catalog", "cat", "catalogs":
		return "catalogs", nil
	case "variant", "var", "variants":
		return "variants", nil
	case "namespace", "ns", "namespaces":
		return "namespaces", nil
	case "view", "v", "views":
		return "views", nil
	case "resource", "res", "resources":
		return "resources", nil
	case "skillset", "sk", "skillsets":
		return "skillsets", nil
	case "session", "sess", "sessions":
		return "sessions", nil
	default:
		return "", fmt.Errorf("unknown resource type: %s", resourceType)
	}
}
