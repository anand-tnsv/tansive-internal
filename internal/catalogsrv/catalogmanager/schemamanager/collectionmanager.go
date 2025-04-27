package schemamanager

import (
	"context"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
)

type CollectionManager interface {
	Schema() string
	Metadata() SchemaMetadata
	FullyQualifiedName() string
	CollectionSchemaManager() CollectionSchemaManager
	CollectionSchema() []byte
	SetCollectionSchemaPath(string)
	GetCollectionSchemaPath() string
	SetCollectionSchemaManager(csm CollectionSchemaManager)
	SetDefaultValues(param ...string) apperrors.Error
	GetValue(ctx context.Context, param string) (types.NullableAny, apperrors.Error)
	GetValueJSON(ctx context.Context, param string) ([]byte, apperrors.Error)
	GetAllValuesJSON(ctx context.Context) ([]byte, apperrors.Error)
	SetValue(ctx context.Context, schemaLoaders SchemaLoaders, param string, value types.NullableAny) apperrors.Error
	ValidateValues(ctx context.Context, schemaLoaders SchemaLoaders, currentValues ParamValues) apperrors.Error
	Values() ParamValues
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
	ToJson(ctx context.Context) ([]byte, apperrors.Error)
}
