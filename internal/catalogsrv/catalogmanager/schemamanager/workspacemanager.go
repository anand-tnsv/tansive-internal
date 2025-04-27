package schemamanager

import (
	"context"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type WorkspaceManager interface {
	ID() uuid.UUID
	Label() string
	Description() string
	VariantID() uuid.UUID
	BaseVersion() int
	ParametersDir() uuid.UUID
	CollectionsDir() uuid.UUID
	ValuesDir() uuid.UUID
	Save(context.Context) apperrors.Error
	ToJson(context.Context) ([]byte, apperrors.Error)
}
