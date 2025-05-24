package schemaresource

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"sigs.k8s.io/yaml"
)

func TestSchemaResource_Validate(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		expected  schemaerr.ValidationErrors
	}{
		{
			name: "valid resource schema",
			yamlInput: `
version: v1
kind: ParameterSchema
metadata:
  name: valid-name
  catalog: valid-catalog
  path: /valid/path
spec:
  description: example spec
`,
			expected: nil,
		},
		{
			name: "invalid name in metadata",
			yamlInput: `
version: v1
kind: ParameterSchema
metadata:
  name: "Invalid Name!" # contains spaces and special characters
  catalog: valid-catalog
  path: /valid/path
spec:
  description: example spec
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidNameFormat("metadata.name", "Invalid Name!"),
			},
		},
		{
			name: "invalid catalog in metadata",
			yamlInput: `
version: v1
kind: ParameterSchema
metadata:
  name: valid-name
  catalog: "Invalid Catalog!" # contains spaces and special characters
  path: /valid/path
spec:
  description: example spec
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidNameFormat("metadata.catalog", "Invalid Catalog!"),
			},
		},
		{
			name: "invalid path in metadata",
			yamlInput: `
version: v1
kind: ParameterSchema
metadata:
  name: valid-name
  catalog: valid-catalog
  path: "invalid/path" # does not start with a slash
spec:
  description: example spec
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidObjectPath("metadata.path"),
			},
		},
		{
			name: "missing required version",
			yamlInput: `
kind: ParameterSchema
metadata:
  name: valid-name
  catalog: valid-catalog
  path: /valid/path
spec:
  description: example spec
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingRequiredAttribute("version"),
			},
		},
		{
			name: "missing required metadata.name",
			yamlInput: `
version: v1
kind: ParameterSchema
metadata:
  catalog: valid-catalog
  path: /valid/path
spec:
  description: example spec
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingRequiredAttribute("metadata.name"),
			},
		},
		{
			name: "missing required metadata.catalog",
			yamlInput: `
version: v1
kind: ParameterSchema
metadata:
  name: valid-name
  path: /valid/path
spec:
  description: example spec
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingRequiredAttribute("metadata.catalog"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Convert YAML input to JSON for unmarshaling into SchemaResource struct
			var input SchemaResource
			jsonData, err := yaml.YAMLToJSON([]byte(tt.yamlInput))
			if err != nil {
				t.Fatalf("failed to convert YAML to JSON: %v", err)
			}

			// Unmarshal JSON into the SchemaResource struct
			err = json.Unmarshal(jsonData, &input)
			if err != nil {
				t.Fatalf("failed to unmarshal JSON input: %v", err)
			}

			// Validate the schema
			actual := input.Validate()
			assert.Equal(t, tt.expected, actual)
		})
	}
}
