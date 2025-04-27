package validationerrors

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	ErrSchemaValidation    apperrors.Error = apperrors.New("error validating schema").SetStatusCode(http.StatusBadRequest)
	ErrEmptySchema         apperrors.Error = ErrSchemaValidation.New("empty schema")
	ErrInvalidVersion      apperrors.Error = ErrSchemaValidation.New("invalid version")
	ErrSchemaSerialization apperrors.Error = ErrSchemaValidation.New("error serializing schema")
	ErrInvalidSchema       apperrors.Error = ErrSchemaValidation.New("invalid schema")
	ErrInvalidNameFormat   apperrors.Error = ErrSchemaValidation.New("invalid name format")

	ErrValueValidation apperrors.Error = apperrors.New("error validating value").SetStatusCode(http.StatusBadRequest)
	ErrInvalidType     apperrors.Error = ErrValueValidation.New("invalid type")
	ErrInvalidKind     apperrors.Error = ErrValueValidation.New("unsupported kind")
	ErrInvalidDataType apperrors.Error = ErrValueValidation.New("unsupported data type")
	ErrValueBelowMin   apperrors.Error = ErrValueValidation.New("value is below minimum")
	ErrValueAboveMax   apperrors.Error = ErrValueValidation.New("value is above maximum")
	ErrValueInvalid    apperrors.Error = ErrValueValidation.New("value failed validation")
	ErrValueNotInStep  apperrors.Error = ErrValueValidation.New("value not in step with min and max values")
)
