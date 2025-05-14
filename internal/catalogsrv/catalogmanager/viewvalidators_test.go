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
		{"res://catalogs/my-catalog", true},
		{"res://catalogs/my-catalog/variants/my-variant", true},
		{"res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace", true},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace/namespaces/my-namespace", true},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace", true},
		{"res://catalogs/my-catalog/variants/my-variant/collections/path", true},
		{"res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/collections/path/to/res-ource", true},
		{"res://catalogs/my-catalog/variants/my-variant/collections/path/*", true},
		{"res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/collections/path/*", true},
		{"res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/workspaces/my-workspace/collections/path/*", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace/namespaces/my-namespace/collections/*", true},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/*", true},
		{"res://catalogs/*", true},
		{"res://catalogs/my-catalog/variants/my-variant/namespaces/*", true},
		{"res://catalogs/*/variants/my-variant", true},

		// Invalid cases - missing required components
		{"res://", false},
		{"res://catalogs/test-catalog/varian/test-variant", false},
		{"res://variants/my-variant", false},     // missing catalog
		{"res://namespaces/my-namespace", false}, // missing catalog and variant
		{"res://workspaces/my-workspace", false}, // missing catalog and variant
		{"res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/*", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace/namespaces/my-namespace/collections/*/path/*", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace/namespaces/my-namespace/*/collections/path", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/namespaces/my-namespace//collections/path", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace/namespaces/my-namespace/resource", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my*", false},
		{"res://catalogs/my-catalog/variants/my-variant/workspaces/my-workspace/namespaces/my-namespace/collections/*/", false},

		// Invalid cases - wrong order
		{"res://variants/my-variant/catalogs/my-catalog", false},
		{"res://namespaces/my-namespace/variants/my-variant", false},
		{"res://workspaces/my-workspace/namespaces/my-namespace", false},

		// Invalid cases - invalid characters
		{"res://catalogs/my@catalog", false},
		{"res://catalogs/my catalog", false},
		{"res://catalogs/my-catalog/variants/my@variant", false},
		{"res://catalogs/my-catalog/variants/my variant", false},
		{"res://catalogs/my-catalog/variants/my_variant", false},

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
