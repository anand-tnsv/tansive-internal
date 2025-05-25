package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

type Resource struct {
	ID        uuid.UUID      `db:"id"`
	Path      string         `db:"path"`
	Hash      string         `db:"hash"`
	VariantID uuid.UUID      `db:"variant_id"`
	TenantID  types.TenantId `db:"tenant_id"`
	CreatedAt time.Time      `db:"created_at"`
	UpdatedAt time.Time      `db:"updated_at"`
}
