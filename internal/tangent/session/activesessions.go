package session

import (
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
	ViewID         uuid.UUID          `json:"view_id"`
	ViewDefinition json.RawMessage    `json:"view_definition"`
	Variables      json.RawMessage    `json:"variables"`
	StatusSummary  string             `json:"status_summary"`
	Status         json.RawMessage    `json:"status"`
	Info           json.RawMessage    `json:"info"`
	UserID         string             `json:"user_id"`
	CatalogID      uuid.UUID          `json:"catalog_id"`
	VariantID      uuid.UUID          `json:"variant_id"`
	TenantID       catcommon.TenantId `json:"tenant_id"`
	CreatedAt      time.Time          `json:"created_at"`
	StartedAt      time.Time          `json:"started_at"`
	EndedAt        time.Time          `json:"ended_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
	ExpiresAt      time.Time          `json:"expires_at"`
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
