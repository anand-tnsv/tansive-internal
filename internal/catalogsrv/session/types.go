package session

import (
	"time"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type SessionStatus string

const (
	SessionStatusCreated    SessionStatus = "created"
	SessionStatusRunning    SessionStatus = "running"
	SessionStatusCompleted  SessionStatus = "completed"
	SessionStatusFailed     SessionStatus = "failed"
	SessionStatusExpired    SessionStatus = "expired"
	SessionStatusCancelled  SessionStatus = "cancelled"
	SessionStatusPaused     SessionStatus = "paused"
	SessionStatusResumed    SessionStatus = "resumed"
	SessionStatusSuspended  SessionStatus = "suspended"
	SessionStatusTerminated SessionStatus = "terminated"
)

type InteractiveSessionRsp struct {
	Code       string `json:"code"`
	TangentURL string `json:"tangent_url"`
}

type SessionTokenRsp struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

type ExecutionState struct {
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
