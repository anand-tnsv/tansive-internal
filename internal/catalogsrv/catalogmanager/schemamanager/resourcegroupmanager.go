package schemamanager

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/objectstore"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ResourceGroupManager interface {
	Metadata() SchemaMetadata
	FullyQualifiedName() string
	GetValue(ctx context.Context, resourceName string) (types.NullableAny, apperrors.Error)
	GetValueJSON(ctx context.Context, resourceName string) ([]byte, apperrors.Error)
	SetValue(ctx context.Context, resourceName string, value types.NullableAny) apperrors.Error
	StorageRepresentation() *objectstore.ObjectStorageRepresentation
	Save(ctx context.Context) apperrors.Error
	GetStoragePath() string
	Delete(ctx context.Context) apperrors.Error
}
