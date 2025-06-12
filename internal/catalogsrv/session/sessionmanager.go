package session

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type SessionManager interface {
	ID() uuid.UUID
	Save(ctx context.Context) apperrors.Error
	GetViewManager(ctx context.Context) (policy.ViewManager, apperrors.Error)
	GetExecutionState(ctx context.Context) *ExecutionState
	SetStatusSummary(ctx context.Context, statusSummary SessionStatus) apperrors.Error
}

func (s *sessionManager) ID() uuid.UUID {
	return s.session.SessionID
}

func (s *sessionManager) GetViewManager(ctx context.Context) (policy.ViewManager, apperrors.Error) {
	return s.viewManager, nil
}

func (s *sessionManager) GetExecutionState(ctx context.Context) *ExecutionState {
	sessionInfo := SessionInfo{}
	err := json.Unmarshal(s.session.Info, &sessionInfo)
	if err != nil {
		return nil
	}
	return &ExecutionState{
		SessionID:        s.session.SessionID,
		SkillSet:         s.session.SkillSet,
		Skill:            s.session.Skill,
		View:             s.viewManager.Name(),
		ViewDefinition:   s.viewManager.GetViewDefinition(),
		SessionVariables: sessionInfo.SessionVariables,
		InputArgs:        sessionInfo.InputArgs,
		Catalog:          s.viewManager.Scope().Catalog,
		Variant:          s.viewManager.Scope().Variant,
		Namespace:        s.viewManager.Scope().Namespace,
		TenantID:         catcommon.GetTenantID(ctx),
	}
}

func (s *sessionManager) SetStatusSummary(ctx context.Context, statusSummary SessionStatus) apperrors.Error {
	s.session.StatusSummary = string(statusSummary)
	err := db.DB(ctx).UpdateSessionStatus(ctx, s.session.SessionID, string(statusSummary), s.session.Status)
	if err != nil {
		return err
	}
	return nil
}
