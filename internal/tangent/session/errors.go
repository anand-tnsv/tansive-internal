package session

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	ErrSessionError              apperrors.Error = apperrors.New("error in processing session").SetStatusCode(http.StatusInternalServerError)
	ErrInvalidSession            apperrors.Error = ErrSessionError.New("invalid session").SetStatusCode(http.StatusBadRequest)
	ErrAlreadyExists             apperrors.Error = ErrSessionError.New("session already exists").SetStatusCode(http.StatusConflict)
	ErrBadRequest                apperrors.Error = apperrors.New("bad request").SetStatusCode(http.StatusBadRequest)
	ErrChannelFailed             apperrors.Error = apperrors.New("channel failed").SetStatusCode(http.StatusInternalServerError)
	ErrUnknownMethod             apperrors.Error = apperrors.New("unknown method").SetStatusCode(http.StatusMethodNotAllowed)
	ErrInvalidParams             apperrors.Error = apperrors.New("invalid parameters").SetStatusCode(http.StatusBadRequest)
	ErrExecutionFailed           apperrors.Error = apperrors.New("execution failed").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToGetSkillset       apperrors.Error = apperrors.New("unable to get skillset").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToGetViewDefinition apperrors.Error = apperrors.New("unable to get view definition").SetStatusCode(http.StatusInternalServerError)
	ErrInvalidObject             apperrors.Error = apperrors.New("invalid object").SetStatusCode(http.StatusBadRequest)
)
