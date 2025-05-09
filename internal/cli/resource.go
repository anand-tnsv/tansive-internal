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

	// Convert YAML to JSON
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert YAML to JSON: %v", err)
	}

	// Parse the resource to get its kind
	var resource Resource
	if err := json.Unmarshal(jsonData, &resource); err != nil {
		return nil, nil, fmt.Errorf("failed to parse resource: %v", err)
	}

	return jsonData, &resource, nil
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
	case "CollectionSchema":
		return "collectionschemas", nil
	case "ParameterSchema":
		return "parameterschemas", nil
	case "Collection":
		return "collections", nil
	default:
		return "", fmt.Errorf("unknown resource kind: %s", kind)
	}
}
