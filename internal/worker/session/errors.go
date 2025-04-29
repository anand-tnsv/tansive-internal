package session

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	ErrSessionError   apperrors.Error = apperrors.New("error in processing session").SetStatusCode(http.StatusInternalServerError)
	ErrInvalidSession apperrors.Error = ErrSessionError.New("invalid session").SetStatusCode(http.StatusBadRequest)
	ErrAlreadyExists  apperrors.Error = ErrSessionError.New("session already exists").SetStatusCode(http.StatusConflict)
	ErrBadRequest     apperrors.Error = apperrors.New("bad request").SetStatusCode(http.StatusBadRequest)
	ErrChannelFailed  apperrors.Error = apperrors.New("channel failed").SetStatusCode(http.StatusInternalServerError)
)
