package session

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type activeSessions struct {
	sessions map[uuid.UUID]*session
}

type session struct {
	id          uuid.UUID
	context     *ServerContext
	skillSet    catalogmanager.SkillSetManager
	viewManager policy.ViewManager
	token       string
}

type ServerContext struct {
	SessionID      uuid.UUID          `json:"session_id"`
	SkillSet       string             `json:"skillset"`
	Skill          string             `json:"skill"`
	View           string             `json:"view"`
	ViewDefinition json.RawMessage    `json:"view_definition"`
	Variables      json.RawMessage    `json:"variables"`
	StatusSummary  string             `json:"status_summary"`
	Status         json.RawMessage    `json:"status"`
	Info           json.RawMessage    `json:"info"`
	UserID         string             `json:"user_id"`
	Catalog        string             `json:"catalog"`
	Variant        string             `json:"variant"`
	Namespace      string             `json:"namespace"`
	TenantID       catcommon.TenantId `json:"tenant_id"`
	CreatedAt      time.Time          `json:"created_at"`
	StartedAt      time.Time          `json:"started_at"`
	EndedAt        time.Time          `json:"ended_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
	ExpiresAt      time.Time          `json:"expires_at"`
}

var sessionManager *activeSessions

func (as *activeSessions) CreateSession(ctx context.Context, c *ServerContext, token string) (*session, apperrors.Error) {
	if c.SessionID == uuid.Nil {
		return nil, ErrInvalidSession
	}
	// if a session with the same ID already exists, return an error
	if _, exists := as.sessions[c.SessionID]; exists {
		return nil, ErrAlreadyExists.New("session already exists")
	}
	session := &session{
		id:          c.SessionID,
		context:     c,
		skillSet:    nil,
		viewManager: nil,
		token:       token,
	}
	as.sessions[c.SessionID] = session
	return session, nil
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
