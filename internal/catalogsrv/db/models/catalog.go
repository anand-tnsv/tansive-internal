package models

import (
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/tansive/tansive-internal/pkg/types"
)

/*
   Column    |          Type           | Collation | Nullable |      Default
-------------+-------------------------+-----------+----------+--------------------
 catalog_id  | uuid                    |           | not null | uuid_generate_v4()
 name        | character varying(128)  |           | not null |
 description | character varying(1024) |           |          |
 info        | jsonb                   |           |          |
 project_id  | character varying(10)   |           | not null |
 tenant_id   | character varying(10)   |           | not null |
*/

// Catalog model definition
type Catalog struct {
	CatalogID   uuid.UUID       `db:"catalog_id"`
	Name        string          `db:"name"`
	Description string          `db:"description"`
	Info        pgtype.JSONB    `db:"info"`
	ProjectID   types.ProjectId `db:"project_id"`
}
