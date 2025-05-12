package schemavalidator

import (
	"errors"
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
//   * Contain only alphanumeric and hyphens
//   * Follow DNS label rules (max 63 chars, no uppercase)
//
// Examples:
//   res://catalog/my-catalog
//   res://catalog/my-catalog/variant/my-variant
//   res://catalog/my-catalog/variant/my-variant/namespace/my-namespace
//   res://catalog/my-catalog/variant/my-variant/workspace/my-workspace
//   res://catalog/my-catalog/variant/my-variant/resource/path
//   res://catalog/my-catalog/variant/my-variant/resource/path/*

// const resourceURIBaseRegex = `^res://(catalog/[a-z0-9]([-a-z0-9]*[a-z0-9])?(/variant/[a-z0-9]([-a-z0-9]*[a-z0-9])?(/namespace/[a-z0-9]([-a-z0-9]*[a-z0-9])?(/workspace/[a-z0-9]([-a-z0-9]*[a-z0-9])?)?)?)?)`

// const resourcePathRegex = `(/resource(/[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)?$`

// const resourcePathWithWildcardRegex = `(/resource(/[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/\*)?$`

// // resourceURIValidator checks if the given URI is a valid resource URI.
// func resourceURIValidator(fl validator.FieldLevel) bool {
// 	uri := fl.Field().String()

// 	// First check if the base URI is valid
// 	if !regexp.MustCompile(resourceURIBaseRegex).MatchString(uri) {
// 		return false
// 	}

// 	// Split the URI into segments and validate each one
// 	segments := strings.Split(uri, "/")
// 	foundResource := false
// 	wildcardFound := false
// 	for i, segment := range segments {
// 		// Skip empty segments and the protocol part
// 		if segment == "" || segment == "res:" {
// 			continue
// 		}
// 		// Skip the protocol part
// 		if strings.HasPrefix(segment, "res:") {
// 			continue
// 		}
// 		// Check if we've found the resource segment
// 		if segment == "resource" {
// 			foundResource = true
// 			continue
// 		}
// 		// Handle wildcard
// 		if segment == "*" {
// 			// Only allow wildcard after resource and at the end
// 			if !foundResource || i != len(segments)-1 || wildcardFound {
// 				return false
// 			}
// 			wildcardFound = true
// 			continue
// 		}
// 		// Validate each segment against DNS label rules
// 		if !regexp.MustCompile(resourceNameRegex).MatchString(segment) {
// 			return false
// 		}
// 	}

// 	// Then check if the resource path is valid (either with or without wildcard)
// 	return regexp.MustCompile(resourcePathRegex).MatchString(uri) || regexp.MustCompile(resourcePathWithWildcardRegex).MatchString(uri)
// }

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

func resourceURIValidator(fl validator.FieldLevel) bool {
	uri := fl.Field().String()
	segments, err := extractSegments(uri)
	if err != nil {
		return false
	}

	if len(segments) == 0 || isValidStructuredPath(segments[0]) != nil {
		return false
	}

	if len(segments) > 1 {
		prefix, hasWildcard := extractPrefixIfWildcard(segments[1])
		if hasWildcard && prefix == "" {
			return true
		}
		return V().Var(prefix, "resourcePathValidator") == nil
	}

	return true
}

func extractSegments(s string) ([]string, error) {
	const prefix = "res://"
	if !strings.HasPrefix(s, prefix) {
		return nil, errors.New("invalid resource string: missing res:// prefix")
	}

	rest := strings.TrimPrefix(s, prefix)
	parts := strings.SplitN(rest, "/resource/", 2)
	segments := []string{}

	segments = append(segments, parts[0])

	if len(parts) == 2 {
		if parts[1] != "" {
			parts[1] = "/" + parts[1]
		}
		segments = append(segments, parts[1])
	}

	return segments, nil
}

func isValidStructuredPath(path string) error {
	if path == "" {
		return errors.New("invalid path: empty")
	}
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments)%2 != 0 {
		return errors.New("invalid path: missing kv pair")
	}
	found := make(map[string]int)
	pos := 0
	for i := 0; i < len(segments) && i+1 < len(segments); i++ {
		key := segments[i]
		value := segments[i+1]
		if key == "" || value == "" {
			return errors.New("invalid path: missing key or value")
		}
		if _, ok := found[key]; ok {
			return errors.New("invalid path: duplicate key")
		}
		if V().Var(value, "resourceNameValidator") != nil {
			return errors.New("invalid path: invalid value")
		}
		found[key] = pos
		pos++
		i++
	}
	for key, pos := range found {
		switch key {
		case "catalog":
			if pos != 0 {
				return errors.New("catalog must be the first segment")
			}
		case "variant":
			if pos != 1 {
				return errors.New("variant must be the second segment")
			}
		case "namespace":
			if pos != 2 {
				return errors.New("namespace must be the third segment")
			}
		case "workspace":
			if _, ok := found["namespace"]; !ok {
				if pos != 2 {
					return errors.New("workspace must be the third segment")
				}
			} else {
				if pos != 3 {
					return errors.New("workspace must be the fourth segment")
				}
			}
		default:
			return errors.New("invalid path: unknown key")
		}
	}

	return nil
}

func extractPrefixIfWildcard(path string) (string, bool) {
	const suffix = "/*"
	if strings.HasSuffix(path, suffix) {
		prefix := strings.TrimSuffix(path, suffix)
		return prefix, true
	}
	return path, false
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
