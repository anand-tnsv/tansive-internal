package models

import "github.com/tansive/tansive-internal/pkg/types"

/*
     Column   |         Type          | Collation | Nullable | Default
--------------+-----------------------+-----------+----------+---------
	hash      | character(128)        |           | not null |
	type      | character varying(64) |           | not null |
	tenant_id | character varying(10) |           | not null |
	data      | bytea                 |           | not null |
*/

type CatalogObject struct {
	Hash     string                  `db:"hash"`
	Type     types.CatalogObjectType `db:"type"`
	Version  string                  `db:"version"`
	TenantID types.TenantId          `db:"tenant_id"`
	Data     []byte                  `db:"data"`
}
