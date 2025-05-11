package schemavalidator

import (
	"regexp"
	"strconv"
	"strings"

	"slices"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

var validKinds = []string{
	types.CatalogKind,
	types.VariantKind,
	types.NamespaceKind,
	types.WorkspaceKind,
	types.ParameterSchemaKind,
	types.CollectionSchemaKind,
	types.CollectionKind,
	types.ViewKind,
}

// kindValidator checks if the given kind is a valid resource kind.
func kindValidator(fl validator.FieldLevel) bool {
	kind := fl.Field().String()
	return slices.Contains(validKinds, kind)
}

const nameRegex = `^[A-Za-z0-9_-]+$`

// nameFormatValidator checks if the given name is alphanumeric with underscores and hyphens.
func nameFormatValidator(fl validator.FieldLevel) bool {
	var str string
	if ns, ok := fl.Field().Interface().(types.NullableString); ok {
		if ns.IsNil() {
			return true
		}
		str = ns.String()
	} else {
		str = fl.Field().String()
	}
	re := regexp.MustCompile(nameRegex)
	return re.MatchString(str)
}

const resourceNameRegex = `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
const resourceNameMaxLength = 63

// resourceNameValidator checks if the given name follows our convention.
func resourceNameValidator(fl validator.FieldLevel) bool {
	var str string
	if ns, ok := fl.Field().Interface().(types.NullableString); ok {
		if ns.IsNil() {
			return true
		}
		str = ns.String()
	} else {
		str = fl.Field().String()
	}

	// Check the length of the name
	if len(str) > resourceNameMaxLength {
		return false
	}

	re := regexp.MustCompile(resourceNameRegex)
	return re.MatchString(str)
}

// resourceURIRegex validates resource URIs with the following structure:
// - Must start with "res://"
// - Must have a location component that follows the hierarchy:
//   * catalog/<name> (required)
//   * /variant/<name> (optional, requires catalog)
//   * /namespace/<name> (optional, requires variant)
//   * /workspace/<name> (optional, requires variant)
// - May have additional path segments after the location
// - All names must:
//   * Start and end with alphanumeric
//   * Contain only alphanumeric, hyphens, and underscores
//   * Follow DNS label rules (max 63 chars, no uppercase)
//
// Examples:
//   res://catalog/my-catalog
//   res://catalog/my-catalog/variant/my-variant
//   res://catalog/my-catalog/variant/my-variant/namespace/my-namespace
//   res://catalog/my-catalog/variant/my-variant/workspace/my-workspace
//   res://catalog/my-catalog/variant/my-variant/resource/path

const resourceURIRegex = `^res://(catalog/[a-z0-9]([-a-z0-9]*[a-z0-9])?(/variant/[a-z0-9]([-a-z0-9]*[a-z0-9])?(/namespace/[a-z0-9]([-a-z0-9]*[a-z0-9])?(/workspace/[a-z0-9]([-a-z0-9]*[a-z0-9])?)?)?)?)(/[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`

// resourceURIValidator checks if the given URI is a valid resource URI.
func resourceURIValidator(fl validator.FieldLevel) bool {
	return regexp.MustCompile(resourceURIRegex).MatchString(fl.Field().String())
}

// notNull checks if a nullable value is not null
func notNull(fl validator.FieldLevel) bool {
	nv, ok := fl.Field().Interface().(types.Nullable)
	if !ok { // not a nullable type
		return true
	}
	return !nv.IsNil()
}

func noSpacesValidator(fl validator.FieldLevel) bool {
	re := regexp.MustCompile(`^[^\s]+$`)
	return re.MatchString(fl.Field().String())
}

// resourcePathValidator checks if the given path is a valid resource path.
func resourcePathValidator(fl validator.FieldLevel) bool {
	path := fl.Field().String()
	// Ensure the path starts with a slash, indicating a root path
	if !strings.HasPrefix(path, "/") {
		return false
	}

	// Split the path by slashes and check each collection name
	collections := strings.Split(path, "/")[1:]
	re := regexp.MustCompile(resourceNameRegex)

	for n, collection := range collections {
		// If a segment is empty, continue (e.g., trailing slash is allowed)
		if collection == "" {
			continue
		}
		if n == 0 && collection == types.DefaultNamespace {
			continue // Skip the first segment if it's the default namespace
		}
		// Validate each folder name using the regex
		if !re.MatchString(collection) {
			return false
		}
	}

	return true
}

func catalogVersionValidator(fl validator.FieldLevel) bool {
	version := fl.Field().String()
	// version should either be an integer or a uuid
	if _, err := strconv.Atoi(version); err == nil {
		return true
	}
	if _, err := uuid.Parse(version); err == nil {
		return true
	}
	return false
}

func requireVersionV1(fl validator.FieldLevel) bool {
	version := fl.Field().String()
	return version == types.VersionV1
}

func ValidateSchemaName(name string) bool {
	re := regexp.MustCompile(resourceNameRegex)
	return re.MatchString(name)
}

func ValidateSchemaKind(kind string) bool {
	return slices.Contains(validKinds, kind)
}

func init() {
	V().RegisterValidation("kindValidator", kindValidator)
	V().RegisterValidation("resourceNameValidator", resourceNameValidator)
	V().RegisterValidation("nameFormatValidator", nameFormatValidator)
	V().RegisterValidation("noSpaces", noSpacesValidator)
	V().RegisterValidation("resourcePathValidator", resourcePathValidator)
	V().RegisterValidation("catalogVersionValidator", catalogVersionValidator)
	V().RegisterValidation("notNull", notNull)
	V().RegisterValidation("requireVersionV1", requireVersionV1)
	V().RegisterValidation("resourceURIValidator", resourceURIValidator)
}
