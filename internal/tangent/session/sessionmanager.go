package session

import (
	"context"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type SessionManager interface {
	CreateSession(context.Context, *ServerContext, string) (*session, apperrors.Error)
	GetSession(uuid.UUID) (*session, apperrors.Error)
	ListSessions() ([]*session, apperrors.Error)
	DeleteSession(uuid.UUID) apperrors.Error
}
