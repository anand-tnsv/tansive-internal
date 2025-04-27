package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/tansive/tansive-internal/pkg/types"
)

/*
  Table "public.versions"
        Column         |           Type           | Collation | Nullable |  Default
-----------------------+--------------------------+-----------+----------+------------
 version_num           | integer                  |           | not null |
 label                 | character varying(128)   |           |          |
 description           | character varying(1024)  |           |          |
 info                  | jsonb                    |           |          |
 parameters_directory  | uuid                     |           |          | uuid_nil()
 collections_directory | uuid                     |           |          | uuid_nil()
 values_directory      | uuid                     |           |          | uuid_nil()
 variant_id            | uuid                     |           | not null |
 tenant_id             | character varying(10)    |           | not null |
 created_at            | timestamp with time zone |           |          | now()
 updated_at            | timestamp with time zone |           |          | now()
Indexes:
    "versions_pkey" PRIMARY KEY, btree (version_num, variant_id, tenant_id)
    "unique_label_variant_tenant" UNIQUE, btree (label, variant_id, tenant_id) WHERE label IS NOT NULL
Check constraints:
    "versions_label_check" CHECK (label IS NULL OR label::text ~ '^[A-Za-z0-9_-]+$'::text)
    "versions_version_num_check" CHECK (version_num > 0)
Foreign-key constraints:
    "versions_variant_id_tenant_id_fkey" FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE
Referenced by:
    TABLE "collections_directory" CONSTRAINT "collections_directory_version_num_variant_id_tenant_id_fkey" FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE
    TABLE "parameters_directory" CONSTRAINT "parameters_directory_version_num_variant_id_tenant_id_fkey" FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE
    TABLE "values_directory" CONSTRAINT "values_directory_version_num_variant_id_tenant_id_fkey" FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE
    TABLE "workspaces" CONSTRAINT "workspaces_base_version_variant_id_tenant_id_fkey" FOREIGN KEY (base_version, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id)
Triggers:
    set_version_num BEFORE INSERT ON versions FOR EACH ROW EXECUTE FUNCTION assign_version_num()
    update_versions_updated_at BEFORE UPDATE ON versions FOR EACH ROW EXECUTE FUNCTION set_updated_at()
*/

type Version struct {
	VersionNum     int            `db:"version_num"`
	Label          string         `db:"label"`
	Description    string         `db:"description"`
	Info           pgtype.JSONB   `db:"info"` // JSONB
	ParametersDir  uuid.UUID      `db:"parameters_directory"`
	CollectionsDir uuid.UUID      `db:"collections_directory"`
	ValuesDir      uuid.UUID      `db:"values_directory"`
	VariantID      uuid.UUID      `db:"variant_id"`
	TenantID       types.TenantId `db:"tenant_id"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
}
