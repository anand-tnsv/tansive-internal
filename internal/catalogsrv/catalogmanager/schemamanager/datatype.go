package schemamanager

import (
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type DataType interface {
	ValidateSpec() schemaerr.ValidationErrors
	ValidateValue(types.NullableAny) apperrors.Error
	GetValue() any
}
