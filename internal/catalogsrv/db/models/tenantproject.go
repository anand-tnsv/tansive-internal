package models

import "github.com/tansive/tansive-internal/pkg/types"

type Tenant struct {
	TenantID types.TenantId
}

type Project struct {
	ProjectID types.ProjectId
	TenantID  types.TenantId
}
