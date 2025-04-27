package parameter

import (
	"encoding/json"
	"testing"

	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/yaml"
)

func TestParameterSchema_Validate(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		expected  schemaerr.ValidationErrors
	}{
		{
			name: "valid parameter schema",
			yamlInput: `
metadata:
  name: valid-name
  catalog: valid-catalog
  path: /valid_path
spec:
  dataType: Integer
`,
			expected: nil,
		},
		{
			name: "missing required data type in spec",
			yamlInput: `
metadata:
  name: valid-name
  catalog: valid-catalog
  path: /valid_path
spec:
  validation: {}
  default: {}
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingRequiredAttribute("spec.dataType"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Convert YAML input to JSON for unmarshaling into struct
			var input ParameterSchema
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
