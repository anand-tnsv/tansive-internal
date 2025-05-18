package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

/*
   Column     |          Type           | Collation | Nullable |      Default
--------------+-------------------------+-----------+----------+--------------------
 key_id       | uuid                    |           | not null | uuid_generate_v4()
 public_key   | bytea                   |           | not null |
 private_key  | bytea                   |           | not null |
 is_active    | boolean                 |           | not null | false
 tenant_id    | character varying(10)   |           | not null |
 created_at   | timestamptz            |           | not null | now()
 updated_at   | timestamptz            |           | not null | now()
Indexes:
    "signing_keys_pkey" PRIMARY KEY, btree (key_id, tenant_id)
    "idx_active_signing_key_per_tenant" UNIQUE, btree (tenant_id) WHERE is_active = true
Foreign-key constraints:
    "signing_keys_tenant_id_fkey" FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
*/

type SigningKey struct {
	KeyID      uuid.UUID      `db:"key_id"`
	PublicKey  []byte         `db:"public_key"`
	PrivateKey []byte         `db:"private_key"`
	IsActive   bool           `db:"is_active"`
	TenantID   types.TenantId `db:"tenant_id"`
	CreatedAt  time.Time      `db:"created_at"`
	UpdatedAt  time.Time      `db:"updated_at"`
}
