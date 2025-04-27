package schemamanager

import (
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type Parameter interface {
	ValidateSpec() schemaerr.ValidationErrors
	ValidateValue(types.NullableAny) apperrors.Error
	DefaultValue() any
}
