package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

/*
      Column       |           Type           | Collation | Nullable |           Default
-------------------+--------------------------+-----------+----------+------------------------------
 collection_id     | uuid                     |           | not null | uuid_generate_v4()
 path              | character varying(512)   |           | not null |
 hash              | character(512)           |           | not null |
 description       | character varying(1024)  |           |          |
 namespace         | character varying(128)   |           | not null | 'default'::character varying
 collection_schema | character varying(512)   |           | not null |
 info              | jsonb                    |           |          |
 repo_id           | uuid                     |           | not null |
 variant_id        | uuid                     |           | not null |
 tenant_id         | character varying(10)    |           | not null |
 created_at        | timestamp with time zone |           |          | now()
 updated_at        | timestamp with time zone |           |          | now()
Indexes:
    "collections_pkey" PRIMARY KEY, btree (collection_id, tenant_id)
    "collections_path_namespace_repo_id_variant_id_tenant_id_key" UNIQUE CONSTRAINT, btree (path, namespace, repo_id, variant_id, tenant_id)
    "idx_schema_namespace_repo_variant_tenant" btree (collection_schema, namespace, repo_id, variant_id, tenant_id)
Check constraints:
    "collections_collection_schema_check" CHECK (collection_schema::text ~ '^[A-Za-z0-9_-]+$'::text)
    "collections_namespace_check" CHECK (namespace::text ~ '^[A-Za-z0-9_-]+$'::text)
    "collections_path_check" CHECK (path::text ~ '^(/[A-Za-z0-9_-]+)+$'::text)
Foreign-key constraints:
    "collections_variant_id_tenant_id_fkey" FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE
*/

type Collection struct {
	CollectionID     uuid.UUID      `db:"collection_id"`
	Path             string         `db:"path"`
	Hash             string         `db:"hash"`
	CollectionSchema string         `db:"collection_schema"`
	RepoID           uuid.UUID      `db:"repo_id"`
	VariantID        uuid.UUID      `db:"variant_id"`
	TenantID         types.TenantId `db:"tenant_id"`
	CreatedAt        time.Time      `db:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at"`
}
