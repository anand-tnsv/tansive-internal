package errors

import (
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
)

var (
	ErrInvalidIntegerType apperrors.Error = validationerrors.ErrInvalidType.New("invalid type for Integer")
)
