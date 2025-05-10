-- Set schema
SET search_path TO public;

-- Drop triggers (you must drop them before dropping the tables)
DROP TRIGGER IF EXISTS update_catalogs_updated_at ON catalogs;
DROP TRIGGER IF EXISTS update_variants_updated_at ON variants;
DROP TRIGGER IF EXISTS update_versions_updated_at ON versions;
DROP TRIGGER IF EXISTS update_workspaces_updated_at ON workspaces;
DROP TRIGGER IF EXISTS set_version_num ON versions;

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
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS tenants CASCADE;

-- Done!
