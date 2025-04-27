package errors

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var ErrV1ObjectError = apperrors.New("v1 object error")
var (
	ErrObjectNotFound     = ErrV1ObjectError.New("object not found").SetStatusCode(http.StatusNotFound)
	ErrUnableToLoadObject = ErrV1ObjectError.New("unable to load object").SetStatusCode(http.StatusInternalServerError)
)
