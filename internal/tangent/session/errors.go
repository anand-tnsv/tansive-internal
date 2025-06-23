package session

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	ErrSessionError                 apperrors.Error = apperrors.New("error in processing session").SetStatusCode(http.StatusInternalServerError)
	ErrInvalidSession               apperrors.Error = ErrSessionError.New("invalid session").SetStatusCode(http.StatusBadRequest)
	ErrAlreadyExists                apperrors.Error = ErrSessionError.New("session already exists").SetStatusCode(http.StatusConflict)
	ErrBadRequest                   apperrors.Error = ErrSessionError.New("bad request").SetStatusCode(http.StatusBadRequest)
	ErrChannelFailed                apperrors.Error = ErrSessionError.New("channel failed").SetStatusCode(http.StatusInternalServerError)
	ErrUnknownMethod                apperrors.Error = ErrSessionError.New("unknown method").SetStatusCode(http.StatusMethodNotAllowed)
	ErrInvalidParams                apperrors.Error = ErrSessionError.New("invalid parameters").SetStatusCode(http.StatusBadRequest)
	ErrExecutionFailed              apperrors.Error = ErrSessionError.New("execution failed").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToGetSkillset          apperrors.Error = ErrSessionError.New("unable to get skillset").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToGetViewDefinition    apperrors.Error = ErrSessionError.New("unable to get view definition").SetStatusCode(http.StatusInternalServerError)
	ErrInvalidObject                apperrors.Error = ErrSessionError.New("invalid object").SetStatusCode(http.StatusBadRequest)
	ErrToolGraphError               apperrors.Error = ErrSessionError.New("tool graph error").SetStatusCode(http.StatusBadRequest)
	ErrInvalidInvocationID          apperrors.Error = ErrSessionError.New("invalid invocation ID").SetStatusCode(http.StatusBadRequest)
	ErrBlockedByPolicy              apperrors.Error = ErrSessionError.New("blocked by policy").SetStatusCode(http.StatusForbidden)
	ErrTokenRequired                apperrors.Error = ErrSessionError.New("token is required").SetStatusCode(http.StatusBadRequest)
	ErrTokenExpired                 apperrors.Error = ErrSessionError.New("token has expired").SetStatusCode(http.StatusBadRequest)
	ErrFailedRequestToTansiveServer apperrors.Error = ErrSessionError.New("failed to make request to Tansive server").SetStatusCode(http.StatusInternalServerError)
	ErrTransformUndefined           apperrors.Error = ErrSessionError.New("transform is undefined").SetStatusCode(http.StatusBadRequest)
)
