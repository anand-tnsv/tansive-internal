package session

import "github.com/tansive/tansive-internal/internal/common/apperrors"

var (
	ErrSessionError   apperrors.Error = apperrors.New("session error")
	ErrInvalidSession apperrors.Error = ErrSessionError.New("invalid session")
	ErrInvalidObject  apperrors.Error = ErrSessionError.New("invalid object")
	ErrInvalidView    apperrors.Error = ErrSessionError.New("invalid view")
)
