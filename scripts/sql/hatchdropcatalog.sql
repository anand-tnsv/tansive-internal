-- Set schema
SET search_path TO public;

-- Drop triggers (you must drop them before dropping the tables)
DROP TRIGGER IF EXISTS update_catalogs_updated_at ON catalogs;
DROP TRIGGER IF EXISTS update_variants_updated_at ON variants;
DROP TRIGGER IF EXISTS update_versions_updated_at ON versions;
DROP TRIGGER IF EXISTS update_workspaces_updated_at ON workspaces;
DROP TRIGGER IF EXISTS set_version_num ON versions;
DROP TRIGGER IF EXISTS update_tenants_updated_at ON tenants;
DROP TRIGGER IF EXISTS update_projects_updated_at ON projects;
DROP TRIGGER IF EXISTS update_catalog_objects_updated_at ON catalog_objects;
DROP TRIGGER IF EXISTS update_collections_directory_updated_at ON collections_directory;
DROP TRIGGER IF EXISTS update_parameters_directory_updated_at ON parameters_directory;
DROP TRIGGER IF EXISTS update_values_directory_updated_at ON values_directory;
DROP TRIGGER IF EXISTS update_view_tokens_updated_at ON view_tokens;
DROP TRIGGER IF EXISTS update_views_updated_at ON views;
DROP TRIGGER IF EXISTS update_signing_keys_updated_at ON signing_keys;

-- Drop functions
DROP FUNCTION IF EXISTS set_updated_at() CASCADE;
DROP FUNCTION IF EXISTS assign_version_num() CASCADE;
DROP FUNCTION IF EXISTS create_version(
  VARCHAR,
  VARCHAR,
  JSONB,
  UUID,
  UUID,
  VARCHAR
) CASCADE;
DROP FUNCTION IF EXISTS create_workspace(
  UUID,
  UUID,
  VARCHAR,
  VARCHAR,
  VARCHAR,
  JSONB
) CASCADE;
DROP FUNCTION IF EXISTS commit_workspace(
  UUID,
  VARCHAR
) CASCADE;
DROP FUNCTION IF EXISTS commit_workspace_directories (
  UUID,
  UUID,
  UUID,
  UUID,
  VARCHAR
) CASCADE;

-- Drop tables (in reverse dependency order)
DROP TABLE IF EXISTS view_tokens CASCADE;
DROP TABLE IF EXISTS views CASCADE;
DROP TABLE IF EXISTS namespaces CASCADE;
DROP TABLE IF EXISTS values_directory CASCADE;
DROP TABLE IF EXISTS parameters_directory CASCADE;
DROP TABLE IF EXISTS collections_directory CASCADE;
DROP TABLE IF EXISTS catalog_objects CASCADE;
DROP TABLE IF EXISTS workspaces CASCADE;
DROP TABLE IF EXISTS version_sequences CASCADE;
DROP TABLE IF EXISTS versions CASCADE;
DROP TABLE IF EXISTS variants CASCADE;
DROP TABLE IF EXISTS catalogs CASCADE;
DROP TABLE IF EXISTS signing_keys CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS tenants CASCADE;

-- Done!
