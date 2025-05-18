package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ViewToken struct {
	TokenID   uuid.UUID      `json:"token_id"`
	ViewID    uuid.UUID      `json:"view_id"`
	TenantID  types.TenantId `json:"tenant_id"`
	ExpireAt  time.Time      `json:"expire_at"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

func (vt *ViewToken) Validate() error {
	if vt.ViewID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("view_id is required")
	}
	return nil
}
