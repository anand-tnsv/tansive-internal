package models

import (
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
)

/*
   Column    |          Type           | Collation | Nullable |      Default
-------------+-------------------------+-----------+----------+--------------------
 variant_id  | uuid                    |           | not null | uuid_generate_v4()
 name        | character varying(128)  |           | not null |
 description | character varying(1024) |           |          |
 info        | jsonb                   |           |          |
 catalog_id  | uuid                    |           | not null |
 tenant_id   | character varying(10)   |           | not null |
*/

// Variant model definition
type Variant struct {
	VariantID   uuid.UUID    `db:"variant_id"`
	Name        string       `db:"name"`
	Description string       `db:"description"`
	Info        pgtype.JSONB `db:"info"`
	CatalogID   uuid.UUID    `db:"catalog_id"`
}
