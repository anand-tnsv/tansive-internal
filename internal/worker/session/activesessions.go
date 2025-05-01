package session

import (
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type activeSessions struct {
	sessions map[uuid.UUID]*session
}

type session struct {
	id      uuid.UUID
	context types.NullableAny
	channel *channel
}

type channel struct {
	sessionId             uuid.UUID
	conn                  *websocket.Conn
	peerHeartBeatInterval time.Duration
	reader                MessageReader
	writer                MessageWriter
}

var sessionManager *activeSessions

func (as *activeSessions) CreateSession(id uuid.UUID, s *session) apperrors.Error {
	if id == uuid.Nil {
		return ErrInvalidSession
	}
	// if a session with the same ID already exists, return an error
	if _, exists := as.sessions[id]; exists {
		return ErrAlreadyExists.New("session already exists")
	}
	as.sessions[s.id] = s
	return nil
}

func (as *activeSessions) GetSession(id uuid.UUID) (*session, apperrors.Error) {
	if session, exists := as.sessions[id]; exists {
		return session, nil
	}
	return nil, ErrInvalidSession
}

func (as *activeSessions) ListSessions() ([]*session, apperrors.Error) {
	var sessionList []*session
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
		sessions: make(map[uuid.UUID]*session),
	}
}

func ActiveSessionManager() SessionManager {
	return sessionManager
}
