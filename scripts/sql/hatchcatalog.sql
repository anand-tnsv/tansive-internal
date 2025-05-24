SET search_path TO public;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS tenants (
  tenant_id VARCHAR(10) PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER update_tenants_updated_at
BEFORE UPDATE ON tenants
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS projects (
  project_id VARCHAR(10),
  tenant_id VARCHAR(10),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (project_id, tenant_id),
  FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
);

CREATE TRIGGER update_projects_updated_at
BEFORE UPDATE ON projects
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS catalogs (
  catalog_id UUID DEFAULT uuid_generate_v4(),
  name VARCHAR(128) NOT NULL,
  description VARCHAR(1024),
  info JSONB,
  project_id VARCHAR(10) NOT NULL,
  tenant_id VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (name, project_id, tenant_id),
  PRIMARY KEY (catalog_id, tenant_id),
  FOREIGN KEY (project_id, tenant_id) REFERENCES projects(project_id, tenant_id) ON DELETE CASCADE,
  CHECK (name ~ '^[A-Za-z0-9_-]+$') -- CHECK constraint to allow only alphanumeric and underscore in name
);

CREATE TRIGGER update_catalogs_updated_at
BEFORE UPDATE ON catalogs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS variants (
  variant_id UUID DEFAULT uuid_generate_v4(),
  name VARCHAR(128) NOT NULL,
  description VARCHAR(1024),
  info JSONB,
  resourcegroups_directory UUID DEFAULT uuid_nil(),
  catalog_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (name, catalog_id, tenant_id),
  PRIMARY KEY (variant_id, tenant_id),
  FOREIGN KEY (catalog_id, tenant_id) REFERENCES catalogs(catalog_id, tenant_id) ON DELETE CASCADE,
  CHECK (name ~ '^[A-Za-z0-9_-]+$') -- CHECK constraint to allow only alphanumeric and underscore in name
);

CREATE TRIGGER update_variants_updated_at
BEFORE UPDATE ON variants
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS catalog_objects (
  hash CHAR(128) NOT NULL,
  type VARCHAR(64) NOT NULL CHECK (type IN ('parameter_schema', 'collection_schema', 'collection', 'resource_group')),
  version VARCHAR(16) NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  data BYTEA NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (hash, tenant_id)
);

CREATE TRIGGER update_catalog_objects_updated_at
BEFORE UPDATE ON catalog_objects
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS resourcegroups_directory ( 
  directory_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  directory JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (directory_id, tenant_id),
  FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE
);

CREATE TRIGGER update_resourcegroups_directory_updated_at
BEFORE UPDATE ON resourcegroups_directory
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_resourcegroups_directory_hash_gin
ON resourcegroups_directory USING GIN (jsonb_path_query_array(directory, '$.*.hash'));

CREATE TABLE IF NOT EXISTS namespaces (
  name VARCHAR(128) NOT NULL,
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  description VARCHAR(1024),
  info JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (name, variant_id, tenant_id),
  FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE,
  CHECK (name ~ '^[A-Za-z0-9_-]+$') -- CHECK constraint to allow only alphanumeric and underscore in name
);

CREATE TRIGGER update_namespaces_updated_at
BEFORE UPDATE ON namespaces
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS views (
  view_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  label VARCHAR(128),
  description VARCHAR(1024),
  info JSONB,
  rules JSONB NOT NULL,
  catalog_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (label, catalog_id, tenant_id),
  PRIMARY KEY (view_id, tenant_id),
  FOREIGN KEY (catalog_id, tenant_id) REFERENCES catalogs(catalog_id, tenant_id) ON DELETE CASCADE,
  CHECK (label IS NULL OR label ~ '^[A-Za-z0-9_-]+$')  -- CHECK constraint to allow only alphanumeric and underscore in label
);

CREATE TRIGGER update_views_updated_at
BEFORE UPDATE ON views
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS view_tokens (
  token_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  view_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  expire_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (token_id, tenant_id)
);

CREATE TRIGGER update_view_tokens_updated_at
BEFORE UPDATE ON view_tokens
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS signing_keys (
  key_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  public_key BYTEA NOT NULL,
  private_key BYTEA NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (key_id)
);

CREATE TRIGGER update_signing_keys_updated_at
BEFORE UPDATE ON signing_keys
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE UNIQUE INDEX idx_active_signing_key
ON signing_keys (is_active)
WHERE is_active = true;

GRANT ALL PRIVILEGES ON TABLE
	tenants,
	projects,
	catalogs,
	variants,
  catalog_objects,
  resourcegroups_directory,
  namespaces,
  views,
  view_tokens,
  signing_keys
TO catalogrw;
