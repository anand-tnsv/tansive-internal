package session

import (
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type SessionManager interface {
	CreateSession(uuid.UUID, *session) apperrors.Error
	GetSession(uuid.UUID) (*session, apperrors.Error)
	ListSessions() ([]*session, apperrors.Error)
	DeleteSession(uuid.UUID) apperrors.Error
}
