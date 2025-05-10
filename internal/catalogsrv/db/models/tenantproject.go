package models

import (
	"time"

	"github.com/tansive/tansive-internal/pkg/types"
)

type Tenant struct {
	TenantID  types.TenantId
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Project struct {
	ProjectID types.ProjectId
	TenantID  types.TenantId
	CreatedAt time.Time
	UpdatedAt time.Time
}
