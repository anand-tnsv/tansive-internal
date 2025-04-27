package session

import (
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type activeSessions struct {
	sessions map[uuid.UUID]*Session
}

type Session struct {
	ID      uuid.UUID
	Context types.NullableAny
}

var sessionManager *activeSessions

func (as *activeSessions) CreateSession(id uuid.UUID, s *Session) apperrors.Error {
	if id == uuid.Nil {
		return ErrInvalidSession
	}
	// if a session with the same ID already exists, return an error
	if _, exists := as.sessions[id]; exists {
		return ErrAlreadyExists.New("session already exists")
	}
	as.sessions[s.ID] = s
	return nil
}

func (as *activeSessions) GetSession(id uuid.UUID) (*Session, apperrors.Error) {
	if session, exists := as.sessions[id]; exists {
		return session, nil
	}
	return nil, ErrInvalidSession
}

func (as *activeSessions) ListSessions() ([]*Session, apperrors.Error) {
	var sessionList []*Session
	for _, session := range as.sessions {
		sessionList = append(sessionList, session)
	}
	return sessionList, nil
}

func (as *activeSessions) DeleteSession(id uuid.UUID) apperrors.Error {
	if _, exists := as.sessions[id]; !exists {
		return ErrInvalidSession
	}
	delete(as.sessions, id)
	return nil
}

func init() {
	sessionManager = &activeSessions{
		sessions: make(map[uuid.UUID]*Session),
	}
}

func ActiveSessionManager() SessionManager {
	return sessionManager
}
