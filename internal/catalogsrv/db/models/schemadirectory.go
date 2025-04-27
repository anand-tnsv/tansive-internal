package models

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

/*
   Column    |         Type          | Collation | Nullable |      Default
--------------+-----------------------+-----------+----------+--------------------
 directory_id | uuid                  |           | not null | uuid_generate_v4()
 version_num  | integer               |           |          |
 workspace_id | uuid                  |           |          |
 variant_id   | uuid                  |           | not null |
 tenant_id    | character varying(10) |           | not null |
 directory    | jsonb                 |           | not null |
Indexes:
    "collections_directory_pkey" PRIMARY KEY, btree (directory_id, tenant_id)
    "idx_collections_directory_directory_gin" gin (directory)
Foreign-key constraints:
    "collections_directory_tenant_id_fkey" FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
    "collections_directory_version_num_variant_id_tenant_id_fkey" FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE
    "collections_directory_workspace_id_tenant_id_fkey" FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE
*/

type SchemaDirectory struct {
	DirectoryID uuid.UUID      `db:"directory_id"`
	VersionNum  int            `db:"version_num"`
	WorkspaceID uuid.UUID      `db:"workspace_id"`
	VariantID   uuid.UUID      `db:"variant_id"`
	TenantID    types.TenantId `db:"tenant_id"`
	Directory   []byte         `db:"directory"` // JSONB
}

type DirectoryID struct {
	ID   uuid.UUID
	Type types.CatalogObjectType
}

type DirectoryIDs []DirectoryID

type ObjectRef struct {
	Hash       string     `json:"hash"`
	References References `json:"references"`  // used for objects that reference other objects, e.g. schemas
	BaseSchema string     `json:"base_schema"` // used for objects that are based on a schema, e.g. collections
}

// we'll keep Reference as a struct for future extensibility at the cost of increased storage space
type Reference struct {
	Name string `json:"name"`
}

type References []Reference
type Directory map[string]ObjectRef

func (r References) Contains(name string) bool {
	for _, ref := range r {
		if ref.Name == name {
			return true
		}
	}
	return false
}

func DirectoryToJSON(directory Directory) ([]byte, error) {
	return json.Marshal(directory)
}

func JSONToDirectory(data []byte) (Directory, error) {
	var directory Directory
	err := json.Unmarshal(data, &directory)
	return directory, err
}

type DirectoryObjectDeleteOptionsSetter interface {
	ReplaceReferencesWithAncestor(bool)
	IgnoreReferences(bool)
	DeleteReferences(bool)
}

type DirectoryObjectDeleteOptions func(DirectoryObjectDeleteOptionsSetter)

func ReplaceReferencesWithAncestor(b bool) DirectoryObjectDeleteOptions {
	return func(s DirectoryObjectDeleteOptionsSetter) {
		s.ReplaceReferencesWithAncestor(b)
	}
}

func IgnoreReferences(b bool) DirectoryObjectDeleteOptions {
	return func(s DirectoryObjectDeleteOptionsSetter) {
		s.IgnoreReferences(b)
	}
}

func DeleteReferences(b bool) DirectoryObjectDeleteOptions {
	return func(s DirectoryObjectDeleteOptionsSetter) {
		s.DeleteReferences(b)
	}
}

/*
Directory is a json that has the following format:
{
	"<path>" : {
		"hash": "<hash>"
	}
	...
}
Here path is the path of the object in the form of /a/b/c/d
*/
