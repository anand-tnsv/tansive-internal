package schemavalidator

import (
	"regexp"
	"strings"

	"slices"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/pkg/types"
)

var validKinds = []string{
	catcommon.CatalogKind,
	catcommon.VariantKind,
	catcommon.NamespaceKind,
	catcommon.ResourceKind,
	catcommon.SkillSetKind,
	catcommon.ViewKind,
}

// kindValidator checks if the given kind is a valid resource kind.
func kindValidator(fl validator.FieldLevel) bool {
	kind := fl.Field().String()
	return slices.Contains(validKinds, kind)
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

const skillNameRegex = `^[a-z0-9](?:[_-]?[a-z0-9]+)*$`
const skillNameMaxLength = 63

// skillNameValidator checks if the given name follows our convention.
func skillNameValidator(fl validator.FieldLevel) bool {
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
	if len(str) > skillNameMaxLength {
		return false
	}

	re := regexp.MustCompile(skillNameRegex)
	return re.MatchString(str)
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

	// Split the path by slashes and check each segment name
	segments := strings.Split(path, "/")[1:]
	re := regexp.MustCompile(resourceNameRegex)

	for _, segment := range segments {
		// If a segment is empty, continue (e.g., trailing slash is allowed)
		if segment == "" {
			continue
		}
		// Validate each folder name using the regex
		if !re.MatchString(segment) {
			return false
		}
	}

	return true
}

func requireVersionV1(fl validator.FieldLevel) bool {
	version := fl.Field().String()
	return version == catcommon.VersionV1
}

func ValidateKindName(name string) bool {
	re := regexp.MustCompile(resourceNameRegex)
	return re.MatchString(name)
}

func ValidateKind(kind string) bool {
	return slices.Contains(validKinds, kind)
}

func ValidatePathSegment(segment string) bool {
	re := regexp.MustCompile(resourceNameRegex)
	return re.MatchString(segment)
}

func init() {
	V().RegisterValidation("kindValidator", kindValidator)
	V().RegisterValidation("resourceNameValidator", resourceNameValidator)
	V().RegisterValidation("noSpaces", noSpacesValidator)
	V().RegisterValidation("resourcePathValidator", resourcePathValidator)
	V().RegisterValidation("notNull", notNull)
	V().RegisterValidation("requireVersionV1", requireVersionV1)
	V().RegisterValidation("skillNameValidator", skillNameValidator)
}
