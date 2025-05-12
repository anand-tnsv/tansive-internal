package catalogmanager

import (
	"testing"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
)

func TestResourceURIValidator(t *testing.T) {
	validate := schemavalidator.V()
	validate.RegisterValidation("resourceURI", validateResourceURI)

	tests := []struct {
		input   string
		isValid bool
	}{
		// Valid cases
		{"res://catalog/my-catalog", true},
		{"res://catalog/my-catalog/variant/my-variant", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace", true},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace/namespace/my-namespace", true},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace", true},
		{"res://catalog/my-catalog/variant/my-variant/resource/path", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/resource/path/to/res-ource", true},
		{"res://catalog/my-catalog/variant/my-variant/resource/path/*", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/resource/path/*", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/resource/path/*", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace/namespace/my-namespace/resource/*", true},
		{"res://catalog/my-catalog/variant/my-variant/workspace/*", true},
		{"res://catalog/*", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/*", true},
		{"res://catalog/*/variant/my-variant", true},

		// Invalid cases - missing required components
		{"res://", false},
		{"res://catalog/test-catalog/varian/test-variant", false},
		{"res://variant/my-variant", false},     // missing catalog
		{"res://namespace/my-namespace", false}, // missing catalog and variant
		{"res://workspace/my-workspace", false}, // missing catalog and variant
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/*", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace/namespace/my-namespace/resource/*/path/*", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace/namespace/my-namespace/*/resource/path", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/namespace/my-namespace//resource/path", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace/namespace/my-namespace/resource", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my*", false},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace/namespace/my-namespace/resource/*/", false},

		// Invalid cases - wrong order
		{"res://variant/my-variant/catalog/my-catalog", false},
		{"res://namespace/my-namespace/variant/my-variant", false},
		{"res://workspace/my-workspace/namespace/my-namespace", false},

		// Invalid cases - invalid characters
		{"res://catalog/my@catalog", false},
		{"res://catalog/my catalog", false},
		{"res://catalog/my-catalog/variant/my@variant", false},
		{"res://catalog/my-catalog/variant/my variant", false},
		{"res://catalog/my-catalog/variant/my_variant", false},

		// Invalid cases - invalid format
		{"res://invalid-uri", false},
		{"res://invalid-uri/", false},
		{"res://invalid-uri/with-spaces", false},
		{"res://invalid-uri/with@chars", false},
	}

	for _, test := range tests {
		err := validate.Var(test.input, "resourceURI")
		if (err == nil) != test.isValid {
			t.Errorf("Expected %v for input '%s', but got %v", test.isValid, test.input, err == nil)
		}
	}
}
