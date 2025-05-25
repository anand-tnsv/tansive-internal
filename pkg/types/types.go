package types

import (
	"slices"

	"github.com/google/uuid"
)

type TenantId string
type ProjectId string
type CatalogId uuid.UUID
type Hash string

const DefaultVariant = "default"
const InitialVersionLabel = "init"
const DefaultNamespace = "--root--"
const DefaultAdminViewLabel = "default-admin-view"

func (u CatalogId) String() string {
	return uuid.UUID(u).String()
}

func (u CatalogId) IsNil() bool {
	return u == CatalogId(uuid.Nil)
}

type TokenType string

const (
	TokenTypeIdentity TokenType = "identity"
	TokenTypeSession  TokenType = "session"
	TokenTypeService  TokenType = "service"
	TokenTypeUnknown  TokenType = "unknown"
)

const (
	CatalogKind          = "Catalog"
	VariantKind          = "Variant"
	NamespaceKind        = "Namespace"
	WorkspaceKind        = "Workspace"
	ParameterSchemaKind  = "ParameterSchema"
	CollectionSchemaKind = "CollectionSchema"
	CollectionKind       = "Collection"
	ResourceKind         = "Resource"
	AttributeKind        = "Attribute"
	ViewKind             = "View"
	InvalidKind          = "InvalidKind"
)

const (
	ResourceNameCatalogs          = "catalogs"
	ResourceNameVariants          = "variants"
	ResourceNameNamespaces        = "namespaces"
	ResourceNameWorkspaces        = "workspaces"
	ResourceNameParameterSchemas  = "parameterschemas"
	ResourceNameCollectionSchemas = "collectionschemas"
	ResourceNameCollections       = "collections"
	ResourceNameAttributes        = "attributes"
	ResourceNameViews             = "views"
	ResourceNameResources         = "resources"
)

func ResourceURIs() []string {
	return []string{
		ResourceNameParameterSchemas,
		ResourceNameCollectionSchemas,
		ResourceNameCollections,
		ResourceNameAttributes,
		ResourceNameViews,
		ResourceNameResources,
	}
}

func Kind(t CatalogObjectType) string {
	switch t {
	case CatalogObjectTypeParameterSchema:
		return ParameterSchemaKind
	case CatalogObjectTypeCollectionSchema:
		return CollectionSchemaKind
	case CatalogObjectTypeCatalogCollection:
		return CollectionKind
	case CatalogObjectTypeResource:
		return ResourceKind
	default:
		return ""
	}
}

func KindFromResourceName(uri string) string {
	switch uri {
	case ResourceNameCatalogs:
		return CatalogKind
	case ResourceNameVariants:
		return VariantKind
	case ResourceNameNamespaces:
		return NamespaceKind
	case ResourceNameWorkspaces:
		return WorkspaceKind
	case ResourceNameParameterSchemas:
		return ParameterSchemaKind
	case ResourceNameCollectionSchemas:
		return CollectionSchemaKind
	case ResourceNameCollections:
		return CollectionKind
	case ResourceNameAttributes:
		return AttributeKind
	case ResourceNameViews:
		return ViewKind
	case ResourceNameResources:
		return ResourceKind
	default:
		return InvalidKind
	}
}

func ResourceNameFromObjectType(t CatalogObjectType) string {
	switch t {
	case CatalogObjectTypeParameterSchema:
		return "parameterschemas"
	case CatalogObjectTypeCollectionSchema:
		return "collectionschemas"
	case CatalogObjectTypeCatalogCollection:
		return "collections"
	case CatalogObjectTypeResource:
		return "resources"
	default:
		return ""
	}
}

var validResourceNameAndMethod = map[string][]string{
	ResourceNameCollections:       {"POST", "GET", "PUT", "DELETE"},
	ResourceNameParameterSchemas:  {"POST", "GET", "PUT", "DELETE"},
	ResourceNameCollectionSchemas: {"POST", "GET", "PUT", "DELETE"},
	ResourceNameAttributes:        {"GET", "POST", "DELETE"},
}

func IsValidResourceNameAndMethod(r string, m string) bool {
	if methods, ok := validResourceNameAndMethod[r]; ok {
		if slices.Contains(methods, m) {
			return true
		}
	}
	return false
}

const (
	VersionV1 = "v1"
)

type CatalogObjectType string

const (
	CatalogObjectTypeInvalid           CatalogObjectType = "invalid"
	CatalogObjectTypeUnknown           CatalogObjectType = "unknown"
	CatalogObjectTypeSchema            CatalogObjectType = "schema"
	CatalogObjectTypeParameterSchema   CatalogObjectType = "parameter_schema"
	CatalogObjectTypeCollectionSchema  CatalogObjectType = "collection_schema"
	CatalogObjectTypeCatalogCollection CatalogObjectType = "collection"
	CatalogObjectTypeResource          CatalogObjectType = "resource"
)

func CatalogObjectTypeFromKind(k string) CatalogObjectType {
	switch k {
	case ParameterSchemaKind:
		return CatalogObjectTypeParameterSchema
	case CollectionSchemaKind:
		return CatalogObjectTypeCollectionSchema
	case CollectionKind:
		return CatalogObjectTypeCatalogCollection
	case ResourceKind:
		return CatalogObjectTypeResource
	default:
		return CatalogObjectTypeInvalid
	}
}

type Nullable interface {
	IsNil() bool
}

var TestContextKey = struct{}{}
