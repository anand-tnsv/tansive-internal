package models

import (
	"time"

	json "github.com/json-iterator/go"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type SkillSet struct {
	ID        uuid.UUID          `db:"id"`
	Path      string             `db:"path"`
	Hash      string             `db:"hash"`
	Metadata  json.RawMessage    `db:"metadata"`
	VariantID uuid.UUID          `db:"variant_id"`
	TenantID  catcommon.TenantId `db:"tenant_id"`
	CreatedAt time.Time          `db:"created_at"`
	UpdatedAt time.Time          `db:"updated_at"`
}
