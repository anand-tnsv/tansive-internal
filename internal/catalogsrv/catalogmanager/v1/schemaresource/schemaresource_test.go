package schemaresource

import (
	"encoding/json"
	"testing"

	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/stretchr/testify/assert"
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

func TestValidateJsonSchema(t *testing.T) {
	t.Skip("skipping test")
	tests := []struct {
		name     string
		input    string
		expected schemaerr.ValidationErrors
	}{
		{
			name: "valid resource schema",
			input: `{
				"version": "v1",
				"kind": "ParameterSchema",
				"metadata": {"name": "example"},
				"spec": {"description": "example spec"}
			}`,
			expected: nil,
		},
		{
			name: "missing required version",
			input: `{
				"kind": "ParameterSchema",
				"metadata": {"name": "example"},
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).version", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "invalid kind type",
			input: `{
				"version": "v1",
				"kind": 123,
				"metadata": {"name": "example"},
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).kind", ErrStr: "invalid type"},
			},
		},
		{
			name: "missing required metadata",
			input: `{
				"version": "v1",
				"kind": "ParameterSchema",
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).metadata", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "missing required spec",
			input: `{
				"version": "v1",
				"kind": "ParameterSchema",
				"metadata": {"name": "example"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).spec", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "invalid metadata type",
			input: `{
				"version": "v1",
				"kind": "ParameterSchema",
				"metadata": "this should be an object",
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).metadata", ErrStr: "invalid type"},
			},
		},
		{
			name: "invalid spec type",
			input: `{
				"version": "v1",
				"kind": "ParameterSchema",
				"metadata": {"name": "example"},
				"spec": "this should be an object"
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).spec", ErrStr: "invalid type"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := schemavalidator.ValidateJsonSchema(SchemaResourceJsonSchema, tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
