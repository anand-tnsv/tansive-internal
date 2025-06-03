package interfaces

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/objectstore"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

// SkillSetManager defines the interface for managing a single skillset.
type SkillSetManager interface {
	Metadata() Metadata
	FullyQualifiedName() string
	Save(ctx context.Context) apperrors.Error
	JSON(ctx context.Context) ([]byte, apperrors.Error)
	SpecJSON(ctx context.Context) ([]byte, apperrors.Error)
	GetStoragePath() string
	StorageRepresentation() *objectstore.ObjectStorageRepresentation
}
