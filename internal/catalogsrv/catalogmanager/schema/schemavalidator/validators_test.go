package schemavalidator

import (
	"testing"

	"github.com/go-playground/validator/v10"
)

func TestResourcePathValidator(t *testing.T) {
	validate := validator.New()
	validate.RegisterValidation("resourcepath", resourcePathValidator)

	tests := []struct {
		input   string
		isValid bool
	}{
		{input: "/valid-path/with-collections", isValid: true},
		{input: "/valid-collection", isValid: true},
		{input: "/invalid-path/with@chars", isValid: false},
		{input: "relative/path", isValid: false},
		{input: "/another-valid-collection/", isValid: true},
		{input: "/collection_with_underscore/anotherCollection", isValid: false},
		{input: "/invalid-collection//double-slash", isValid: true},
		{input: "/", isValid: true},
		{input: "", isValid: false},
	}

	for _, test := range tests {
		err := validate.Var(test.input, "resourcepath")
		if (err == nil) != test.isValid {
			t.Errorf("Expected %v for input '%s', but got %v", test.isValid, test.input, err == nil)
		}
	}
}

func TestNoSpacesValidator(t *testing.T) {
	validate := validator.New()
	validate.RegisterValidation("noSpaces", noSpacesValidator)

	// Test cases
	tests := []struct {
		input    string
		expected bool
	}{
		{"ValidString", true},        // No spaces, valid string
		{"Invalid String", false},    // Contains spaces
		{"Invalid\tTab", false},      // Contains tab
		{"Invalid\nNewline", false},  // Contains newline
		{"AnotherValidString", true}, // No spaces
		{"", false},                  // Empty string, should fail
		{"Multiple   Spaces", false}, // Multiple spaces
	}

	for _, test := range tests {
		err := validate.Var(test.input, "noSpaces")
		result := err == nil

		if result != test.expected {
			t.Errorf("Expected %v for input '%s', got %v", test.expected, test.input, result)
		}
	}
}

// resourceURIs are of the format "res://<location>/resource/path/to/resource"
// <location> is /catalog/some-catalog-name or /catalog/some-catalog-name/variant/some-variant-name or
// /catalog/some-catalog-name/variant/some-variant-name/namespace/some-namespace-name/workspace/some-workspace-name
// the hierarchy is always catalog/variant/namespace/workspace and it could be a combination of some of them
// if there is a variant, then there must be a catalog. If there is a namespace, then there must be a variant.
// if there is a workspace, there need or need not be a namespace, but there must be a variant.
// there must always be a location before resource

func TestResourceURIValidator(t *testing.T) {
	validate := validator.New()
	validate.RegisterValidation("resourcePathValidator", resourcePathValidator)
	validate.RegisterValidation("resourceNameValidator", resourceNameValidator)
	validate.RegisterValidation("resourceURI", resourceURIValidator)

	tests := []struct {
		input   string
		isValid bool
	}{
		// Valid cases
		{"res://catalog/my-catalog", true},
		{"res://catalog/my-catalog/variant/my-variant", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace", true},
		{"res://catalog/my-catalog/variant/my-variant/workspace/my-workspace", true},
		{"res://catalog/my-catalog/variant/my-variant/resource/path", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/resource/path/to/res-ource", true},
		{"res://catalog/my-catalog/variant/my-variant/resource/path/*", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/resource/path/*", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/resource/path/*", true},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/resource/*", true},

		// // Invalid cases - missing required components
		{"res://", false},
		{"res://catalog/test-catalog/varian/test-variant", false},
		{"res://variant/my-variant", false},     // missing catalog
		{"res://namespace/my-namespace", false}, // missing catalog and variant
		{"res://workspace/my-workspace", false}, // missing catalog and variant
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/resource/*/path/*", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/*/resource/path", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace//resource/path", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/resource", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/*", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/*", false},
		{"res://catalog/*", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/*", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my*", false},
		{"res://catalog/my-catalog/variant/my-variant/namespace/my-namespace/workspace/my-workspace/resource/*/", false},

		// // Invalid cases - wrong order
		{"res://variant/my-variant/catalog/my-catalog", false},
		{"res://namespace/my-namespace/variant/my-variant", false},
		{"res://workspace/my-workspace/namespace/my-namespace", false},

		// // Invalid cases - invalid characters
		{"res://catalog/my@catalog", false},
		{"res://catalog/my catalog", false},
		{"res://catalog/my-catalog/variant/my@variant", false},
		{"res://catalog/my-catalog/variant/my variant", false},
		{"res://catalog/my-catalog/variant/my_variant", false},

		// // Invalid cases - invalid format
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
