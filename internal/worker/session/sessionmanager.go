package session

import (
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type SessionManager interface {
	CreateSession(uuid.UUID, *Session) apperrors.Error
	GetSession(uuid.UUID) (*Session, apperrors.Error)
	ListSessions() ([]*Session, apperrors.Error)
	DeleteSession(uuid.UUID) apperrors.Error
}
