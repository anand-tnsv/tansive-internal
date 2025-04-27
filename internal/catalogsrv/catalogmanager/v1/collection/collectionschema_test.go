package collection

import (
	"encoding/json"
	"testing"

	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/yaml"
)

func TestCollectionSchema_Validate(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		expected  schemaerr.ValidationErrors
	}{
		{
			name: "valid collection schema with schema",
			yamlInput: `
version: v1
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      schema: IntegerParamSchema
      default: 'hello'
  collections:
    databaseConfig:
      schema: DatabaseConfigCollection
`,
			expected: nil,
		},
		{
			name: "valid collection schema with dataType",
			yamlInput: `
version: v1
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      dataType: Integer
      default: 10
  collections:
    databaseConfig:
      schema: DatabaseConfigCollection
`,
			expected: nil,
		},
		{
			name: "missing both schema and dataType",
			yamlInput: `
version: v1
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      default: 5
  collections:
    databaseConfig:
      schema: DatabaseConfigCollection
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingSchemaOrType("spec.parameters.maxRetries.schema"),
				schemaerr.ErrMissingSchemaOrType("spec.parameters.maxRetries.dataType"),
			},
		},
		{
			name: "both schema and dataType present",
			yamlInput: `
version: v1
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      schema: IntegerParamSchema
      dataType: Integer
      default: 5
  collections:
    databaseConfig:
      schema: DatabaseConfigCollection
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrShouldContainSchemaOrType("spec.parameters.maxRetries.dataType"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Convert YAML input to JSON for unmarshaling into struct
			var input CollectionSchema
			jsonData, err := yaml.YAMLToJSON([]byte(tt.yamlInput))
			if err != nil {
				t.Fatalf("failed to convert YAML to JSON: %v", err)
			}

			// Unmarshal JSON into the CollectionSchema struct
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
