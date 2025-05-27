package interfaces

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type NamespaceManager interface {
	Name() string
	Description() string
	Catalog() string
	Variant() string
	GetNamespaceModel() *models.Namespace
	Save(context.Context) apperrors.Error
	ToJson(context.Context) ([]byte, apperrors.Error)
}
