package session

import (
	"context"
	"time"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/session/toolgraph"
)

type activeSessions struct {
	sessions map[uuid.UUID]*session
}

type ServerContext struct {
	SessionID        uuid.UUID              `json:"session_id"`
	SkillSet         string                 `json:"skillset"`
	Skill            string                 `json:"skill"`
	View             string                 `json:"view"`
	ViewDefinition   *policy.ViewDefinition `json:"view_definition"`
	SessionVariables map[string]any         `json:"session_variables"`
	InputArgs        map[string]any         `json:"input_args"`
	Catalog          string                 `json:"catalog"`
	Variant          string                 `json:"variant"`
	Namespace        string                 `json:"namespace"`
	TenantID         catcommon.TenantId     `json:"tenant_id"`
}

var sessionManager *activeSessions

func (as *activeSessions) CreateSession(ctx context.Context, c *ServerContext, token string, tokenExpiry time.Time) (*session, apperrors.Error) {
	if c.SessionID == uuid.Nil {
		return nil, ErrInvalidSession
	}
	// if a session with the same ID already exists, return an error
	if _, exists := as.sessions[c.SessionID]; exists {
		return nil, ErrAlreadyExists.New("session already exists")
	}
	session := &session{
		id:            c.SessionID,
		context:       c,
		skillSet:      nil,
		viewDef:       nil,
		token:         token,
		tokenExpiry:   tokenExpiry,
		callGraph:     toolgraph.NewCallGraph(3), // max depth of 3
		invocationIDs: make(map[string]*policy.ViewDefinition),
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
