package interfaces

import (
	"context"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type VariantManager interface {
	ID() uuid.UUID
	Name() string
	Description() string
	CatalogID() uuid.UUID
	Save(context.Context) apperrors.Error
	ToJson(context.Context) ([]byte, apperrors.Error)
}
