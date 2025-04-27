package integer

import (
	"encoding/json"
	"testing"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
)

func TestIntegerSpec(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		expected  schemaerr.ValidationErrors
	}{
		{
			name: "valid integer spec",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 1,
					"maxValue": 11,
					"step": 2
				},
				"default": 5
			}`,
			expected: nil,
		},
		{
			name: "default value above maxValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 1,
					"maxValue": 11,
					"step": 2
				},
				"default": 13
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "default", ErrStr: validationerrors.ErrValueAboveMax.Error()},
			},
		},
		{
			name: "default value below minValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 3,
					"maxValue": 10
				},
				"default": 1
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "default", ErrStr: validationerrors.ErrValueBelowMin.Error()},
			},
		},
		{
			name: "default value not in step",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 2,
					"maxValue": 10,
					"step": -2
				},
				"default": 7
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "default", ErrStr: validationerrors.ErrValueNotInStep.Error()},
			},
		},
		{
			name: "minValue greater than maxValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 10,
					"maxValue": 5
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMaxValueLessThanMinValue("validation.maxValue"),
			},
		},
		{
			name: "step value without minValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"step": 2
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidStepValue("validation.step"),
			},
		},
		{
			name: "step value is zero",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 1,
					"maxValue": 10,
					"step": 0
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidStepValue("validation.step"),
			},
		},
		{
			name: "step value exceeds maxValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 1,
					"maxValue": 5,
					"step": 6
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidStepValue("validation.step"),
			},
		},
		{
			name: "step value adds up to be less than minValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 1,
					"maxValue": 5,
					"step": -6
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidStepValue("validation.step"),
			},
		},
		{
			name: "negative step value without maxValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 1,
					"step": -2
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidStepValue("validation.step"),
			},
		},
		{
			name: "missing both minValue and maxValue with step",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"step": 3
				}
			}`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidStepValue("validation.step"),
			},
		},
		{
			name: "valid negative step with maxValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"maxValue": 10,
					"step": -2
				}
			}`,
			expected: nil,
		},
		{
			name: "minValue equals maxValue",
			jsonInput: `{
				"dataType": "Integer",
				"validation": {
					"minValue": 5,
					"maxValue": 5
				}
			}`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input Spec
			err := json.Unmarshal([]byte(tt.jsonInput), &input)
			if err != nil {
				t.Fatalf("failed to unmarshal JSON input: %v", err)
			}

			result := input.ValidateSpec()
			if len(result) != len(tt.expected) {
				t.Errorf("expected %v errors, got %v errors", len(tt.expected), len(result))
				t.Errorf("Expected: %v", tt.expected)
				t.Errorf("Got: %v", result)
			}
			for i, err := range result {
				if err.Field != tt.expected[i].Field || err.ErrStr != tt.expected[i].ErrStr {
					t.Errorf("expected error %v, got %v", tt.expected[i], err)
				}
			}
		})
	}
}
