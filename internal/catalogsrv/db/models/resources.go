package models

import (
	"time"

	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
)

type Resource struct {
	ID        uuid.UUID          `db:"id"`
	Path      string             `db:"path"`
	Hash      string             `db:"hash"`
	VariantID uuid.UUID          `db:"variant_id"`
	TenantID  catcommon.TenantId `db:"tenant_id"`
	CreatedAt time.Time          `db:"created_at"`
	UpdatedAt time.Time          `db:"updated_at"`
}
