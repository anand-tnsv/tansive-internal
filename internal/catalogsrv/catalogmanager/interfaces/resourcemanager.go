package interfaces

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/objectstore"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ResourceManager interface {
	Metadata() Metadata
	FullyQualifiedName() string
	GetValue(ctx context.Context) types.NullableAny
	GetValueJSON(ctx context.Context) ([]byte, apperrors.Error)
	SetValue(ctx context.Context, value types.NullableAny) apperrors.Error
	StorageRepresentation() *objectstore.ObjectStorageRepresentation
	Save(ctx context.Context) apperrors.Error
	GetStoragePath() string
	JSON(ctx context.Context) ([]byte, apperrors.Error)
	SpecJSON(ctx context.Context) ([]byte, apperrors.Error)
}
