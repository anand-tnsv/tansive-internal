package schemamanager

import (
	"context"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ParameterSpec struct {
	Name    string
	Default types.NullableAny
	Value   types.NullableAny
}

type CollectionSchemaManager interface {
	ParameterNames() []string
	ParametersWithSchema(schemaName string) []ParameterSpec
	ValidateDependencies(context.Context, SchemaLoaders, SchemaReferences) (SchemaReferences, apperrors.Error)
	ValidateValue(ctx context.Context, loaders SchemaLoaders, param string, value types.NullableAny) apperrors.Error
	SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error
	GetValue(ctx context.Context, param string) ParamValue
	GetDefaultValues() map[string]ParamValue
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
	SetDefaultValues(ctx context.Context)
}
