package schemamanager

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
)

type SchemaManager interface {
	Version() string
	Kind() string
	Type() catcommon.CatalogObjectType
	ParameterSchemaManager() ParameterSchemaManager
	CollectionSchemaManager() CollectionSchemaManager
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
	Metadata() SchemaMetadata
	Name() string
	Path() string
	FullyQualifiedName() string
	Catalog() string
	Description() string
	SetName(name string)
	SetPath(path string)
	SetCatalog(catalog string)
	SetDescription(description string)
	SetMetadata(m *SchemaMetadata)
	ToJson(ctx context.Context) ([]byte, apperrors.Error)
	Compare(other SchemaManager, excludeMetadata bool) bool
}

type ClosestParentSchemaFinder func(ctx context.Context, t catcommon.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error)
type ParameterReferenceForName func(name string) string
type SchemaLoaderByPath func(ctx context.Context, t catcommon.CatalogObjectType, m *SchemaMetadata) (SchemaManager, apperrors.Error)
type SchemaLoaderByHash func(ctx context.Context, t catcommon.CatalogObjectType, hash string, m *SchemaMetadata) (SchemaManager, apperrors.Error)
type SelfMetadata func() SchemaMetadata

type SchemaLoaders struct {
	ByPath        SchemaLoaderByPath
	ByHash        SchemaLoaderByHash
	ClosestParent ClosestParentSchemaFinder
	ParameterRef  ParameterReferenceForName
	SelfMetadata  SelfMetadata
}
