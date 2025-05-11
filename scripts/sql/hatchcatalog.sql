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

CREATE TABLE IF NOT EXISTS versions (
  version_num INT NOT NULL,
  label VARCHAR(128),
  description VARCHAR(1024),
  info JSONB,
  parameters_directory UUID DEFAULT uuid_nil(),
  collections_directory UUID DEFAULT uuid_nil(),
  values_directory UUID DEFAULT uuid_nil(),
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE,
  PRIMARY KEY (version_num, variant_id, tenant_id),
  CHECK (version_num > 0),  -- CHECK constraint to ensure version_num is positive
  CHECK (label IS NULL OR label ~ '^[A-Za-z0-9_-]+$')  -- CHECK constraint to allow only alphanumeric and underscore in label
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_label_variant_tenant
ON versions (label, variant_id, tenant_id)
WHERE label IS NOT NULL;

CREATE TRIGGER update_versions_updated_at
BEFORE UPDATE ON versions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS version_sequences (
  variant_id UUID,
  tenant_id VARCHAR(10) NOT NULL,
  last_value INT DEFAULT 0,
  FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE,
  PRIMARY KEY (variant_id, tenant_id)
);

CREATE OR REPLACE FUNCTION assign_version_num()
RETURNS TRIGGER AS $$
BEGIN
  -- If version_num is already provided, do nothing and return
  IF NEW.version_num IS NOT NULL THEN
    RETURN NEW;
  END IF;

  -- Lock the version_sequences row to prevent race conditions
  LOOP
    -- Try to update the last_value for the existing combination (with locking)
    UPDATE version_sequences
    SET last_value = last_value + 1
    WHERE variant_id = NEW.variant_id AND tenant_id = NEW.tenant_id
    RETURNING last_value INTO NEW.version_num;

    -- If update was successful, exit loop
    IF FOUND THEN
      EXIT;
    END IF;

    -- If no sequence exists, insert a new sequence with version_num as 1
    BEGIN
      INSERT INTO version_sequences (variant_id, tenant_id, last_value)
      VALUES (NEW.variant_id, NEW.tenant_id, 1)
      RETURNING last_value INTO NEW.version_num;
      EXIT;  -- Exit loop if insert is successful

    EXCEPTION WHEN unique_violation THEN
      -- If another transaction inserted the sequence, loop will retry the update
    END;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_version_num
BEFORE INSERT ON versions
FOR EACH ROW
EXECUTE FUNCTION assign_version_num();

CREATE TABLE IF NOT EXISTS workspaces (
  workspace_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  label VARCHAR(128),
  description VARCHAR(1024),
  info JSONB,
  base_version INT NOT NULL,
  parameters_directory UUID DEFAULT uuid_nil(),
  collections_directory UUID DEFAULT uuid_nil(),
  values_directory UUID DEFAULT uuid_nil(),
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (label, variant_id, tenant_id),
  FOREIGN KEY (variant_id, tenant_id) REFERENCES variants(variant_id, tenant_id) ON DELETE CASCADE,
  FOREIGN KEY (base_version, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id),
  PRIMARY KEY (workspace_id, tenant_id),
  CHECK (label IS NULL OR label ~ '^[A-Za-z0-9_-]+$')  -- CHECK constraint to allow only alphanumeric and underscore in label
);

CREATE TRIGGER update_workspaces_updated_at
BEFORE UPDATE ON workspaces
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS catalog_objects (
  hash CHAR(128) NOT NULL,
  type VARCHAR(64) NOT NULL CHECK (type IN ('parameter_schema', 'collection_schema', 'collection')),
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

CREATE TABLE IF NOT EXISTS collections_directory ( 
  directory_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  version_num INT NULL,
  workspace_id UUID NULL,
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (directory_id, tenant_id),
  FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE,
  directory JSONB NOT NULL
);

CREATE TRIGGER update_collections_directory_updated_at
BEFORE UPDATE ON collections_directory
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_collections_directory_hash_gin
ON collections_directory USING GIN (jsonb_path_query_array(directory, '$.*.hash'));

CREATE INDEX IF NOT EXISTS idx_collections_directory_baseschema_gin
ON collections_directory USING GIN (jsonb_path_query_array(directory, '$.*.base_schema'));

CREATE TABLE IF NOT EXISTS parameters_directory (
  directory_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  version_num INT NULL,
  workspace_id UUID NULL,
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (directory_id, tenant_id),
  FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE,
  directory JSONB NOT NULL
);

CREATE TRIGGER update_parameters_directory_updated_at
BEFORE UPDATE ON parameters_directory
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_parameters_directory_hash_gin
ON parameters_directory USING GIN (jsonb_path_query_array(directory, '$.*.hash'));

CREATE INDEX IF NOT EXISTS idx_parameters_directory_baseschema_gin
ON parameters_directory USING GIN (jsonb_path_query_array(directory, '$.*.base_schema'));

CREATE TABLE IF NOT EXISTS values_directory (
  directory_id UUID NOT NULL DEFAULT uuid_generate_v4(),
  version_num INT NULL,
  workspace_id UUID NULL,
  variant_id UUID NOT NULL,
  tenant_id VARCHAR(10) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (directory_id, tenant_id),
  FOREIGN KEY (version_num, variant_id, tenant_id) REFERENCES versions(version_num, variant_id, tenant_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE,
  directory JSONB NOT NULL
);

CREATE TRIGGER update_values_directory_updated_at
BEFORE UPDATE ON values_directory
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_values_directory_hash_gin
ON values_directory USING GIN (jsonb_path_query_array(directory, '$.*.hash'));

CREATE INDEX IF NOT EXISTS idx_values_directory_baseschema_gin
ON values_directory USING GIN (jsonb_path_query_array(directory, '$.*.base_schema'));

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

CREATE OR REPLACE FUNCTION create_version(
  in_label VARCHAR,
  in_description VARCHAR,
  in_info JSONB,
  in_base_variant_id UUID,
  in_variant_id UUID,
  in_tenant_id VARCHAR
)
RETURNS TABLE (
  version_num INT,
  label VARCHAR,
  description VARCHAR,
  info JSONB,
  parameters_directory UUID,
  collections_directory UUID,
  values_directory UUID,
  variant_id UUID,
  tenant_id VARCHAR,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_collections_dir_id UUID;
  v_parameters_dir_id UUID;
  v_values_dir_id UUID;
  v_collections JSONB;
  v_parameters JSONB;
  v_values JSONB;
  v_version_num INT;
  new_collections_id UUID := uuid_generate_v4();
  new_parameters_id UUID := uuid_generate_v4();
  new_values_id UUID := uuid_generate_v4();
BEGIN
  -- Fetch the directory UUIDs from base variant
  IF in_base_variant_id IS NOT NULL THEN
    SELECT v.collections_directory, v.parameters_directory, v.values_directory
    INTO STRICT v_collections_dir_id, v_parameters_dir_id, v_values_dir_id
    FROM versions v
    WHERE v.version_num = 1
      AND v.variant_id = in_base_variant_id
      AND v.tenant_id = in_tenant_id;

    -- Load actual directories
    SELECT cd.directory INTO STRICT v_collections
    FROM collections_directory cd
    WHERE cd.directory_id = v_collections_dir_id
      AND cd.tenant_id = in_tenant_id;

    SELECT pd.directory INTO STRICT v_parameters
    FROM parameters_directory pd
    WHERE pd.directory_id = v_parameters_dir_id
      AND pd.tenant_id = in_tenant_id;

    SELECT vd.directory INTO STRICT v_values
    FROM values_directory vd
    WHERE vd.directory_id = v_values_dir_id
      AND vd.tenant_id = in_tenant_id;
  ELSE
    -- If no base variant is provided, use empty directories
    v_collections := '{}'::jsonb;
    v_parameters := '{}'::jsonb;
    v_values := '{}'::jsonb;
  END IF;

  -- Insert the version first to satisfy foreign keys
  INSERT INTO versions (
    label, description, info, parameters_directory, collections_directory, values_directory, variant_id, tenant_id
	) VALUES (
    in_label, in_description, in_info, new_parameters_id, new_collections_id, new_values_id, in_variant_id, in_tenant_id
  )
  RETURNING versions.version_num INTO v_version_num;

  -- Now insert collections_directory referencing workspace_id
  INSERT INTO collections_directory (
    directory_id, version_num, workspace_id, variant_id, tenant_id, directory
  ) VALUES (
    new_collections_id, 1, NULL, in_variant_id, in_tenant_id, v_collections
  );

  -- Now insert parameters_directory referencing workspace_id
  INSERT INTO parameters_directory (
    directory_id, version_num, workspace_id, variant_id, tenant_id, directory
  ) VALUES (
    new_parameters_id, 1, NULL, in_variant_id, in_tenant_id, v_parameters
  );

  -- Now insert values_directory referencing workspace_id
  INSERT INTO values_directory (
    directory_id, version_num, workspace_id, variant_id, tenant_id, directory
  ) VALUES (
    new_values_id, 1, NULL, in_variant_id, in_tenant_id, v_values
  );

  -- Return the full version
  RETURN QUERY
  SELECT
    v.version_num, v.label, v.description, v.info,
    v.parameters_directory, v.collections_directory, v.values_directory,
    v.variant_id, v.tenant_id, v.created_at, v.updated_at
  FROM versions v
  WHERE v.version_num = v_version_num AND v.variant_id = in_variant_id AND v.tenant_id = in_tenant_id;
END;
$$;

CREATE OR REPLACE FUNCTION create_workspace(
  in_workspace_id UUID,
  in_variant_id UUID,
  in_tenant_id VARCHAR,
  in_label VARCHAR,
  in_description VARCHAR,
  in_info JSONB
)
RETURNS TABLE (
  workspace_id UUID,
  label VARCHAR,
  description VARCHAR,
  info JSONB,
  base_version INT,
  parameters_directory UUID,
  collections_directory UUID,
  values_directory UUID,
  variant_id UUID,
  tenant_id VARCHAR,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_collections_dir_id UUID;
  v_parameters_dir_id UUID;
  v_values_dir_id UUID;
  v_collections JSONB;
  v_parameters JSONB;
  v_values JSONB;
  new_workspace_id UUID;
  new_collections_id UUID := uuid_generate_v4();
  new_parameters_id UUID := uuid_generate_v4();
  new_values_id UUID := uuid_generate_v4();
BEGIN
  IF in_workspace_id IS NULL THEN
    new_workspace_id := uuid_generate_v4();
  ELSE
    new_workspace_id := in_workspace_id;
  END IF;
  -- Fetch the directory UUIDs from version 1
  SELECT v.collections_directory, v.parameters_directory, v.values_directory
  INTO STRICT v_collections_dir_id, v_parameters_dir_id, v_values_dir_id
  FROM versions v
  WHERE v.version_num = 1
    AND v.variant_id = in_variant_id
    AND v.tenant_id = in_tenant_id;

  -- Load actual directories
  SELECT cd.directory INTO STRICT v_collections
  FROM collections_directory cd
  WHERE cd.directory_id = v_collections_dir_id
    AND cd.tenant_id = in_tenant_id;

  SELECT pd.directory INTO STRICT v_parameters
  FROM parameters_directory pd
  WHERE pd.directory_id = v_parameters_dir_id
    AND pd.tenant_id = in_tenant_id;

  SELECT vd.directory INTO STRICT v_values
  FROM values_directory vd
  WHERE vd.directory_id = v_values_dir_id
    AND vd.tenant_id = in_tenant_id;

  -- Insert the workspace first to satisfy foreign keys
  INSERT INTO workspaces (
    workspace_id, label, description, info, base_version,
    parameters_directory, collections_directory, values_directory,
    variant_id, tenant_id
  ) VALUES (
    new_workspace_id, in_label, in_description, in_info, 1,
    new_parameters_id, new_collections_id, new_values_id,
    in_variant_id, in_tenant_id
  );

  -- Now insert collections_directory referencing workspace_id
  INSERT INTO collections_directory (
    directory_id, version_num, workspace_id, variant_id, tenant_id, directory
  ) VALUES (
    new_collections_id, NULL, new_workspace_id, in_variant_id, in_tenant_id, v_collections
  );

  -- Now insert parameters_directory referencing workspace_id
  INSERT INTO parameters_directory (
    directory_id, version_num, workspace_id, variant_id, tenant_id, directory
  ) VALUES (
    new_parameters_id, NULL, new_workspace_id, in_variant_id, in_tenant_id, v_parameters
  );

  -- Now insert values_directory referencing workspace_id
  INSERT INTO values_directory (
    directory_id, version_num, workspace_id, variant_id, tenant_id, directory
  ) VALUES (
    new_values_id, NULL, new_workspace_id, in_variant_id, in_tenant_id, v_values
  );

  -- Return the full workspace
  RETURN QUERY
  SELECT
    w.workspace_id, w.label, w.description, w.info, w.base_version,
    w.parameters_directory, w.collections_directory, w.values_directory,
    w.variant_id, w.tenant_id, w.created_at, w.updated_at
  FROM workspaces w
  WHERE w.workspace_id = new_workspace_id AND w.tenant_id = in_tenant_id;
END;
$$;

CREATE OR REPLACE FUNCTION commit_workspace(
  in_workspace_id UUID,
  in_tenant_id VARCHAR
)
RETURNS TABLE (
  workspace_id UUID,
  variant_id UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
  w_variant_id UUID;
  w_collections_dir_id UUID;
  w_parameters_dir_id UUID;
  w_values_dir_id UUID;
BEGIN
  -- Fetch directory IDs and variant from workspace
  SELECT w.collections_directory, w.parameters_directory, w.values_directory, w.variant_id
  INTO STRICT w_collections_dir_id, w_parameters_dir_id, w_values_dir_id, w_variant_id
  FROM workspaces w
  WHERE w.workspace_id = in_workspace_id
    AND w.tenant_id = in_tenant_id;

  -- Commit the workspace directories to version 1
  PERFORM commit_workspace_directories(
    w_collections_dir_id, w_parameters_dir_id, w_values_dir_id, w_variant_id, in_tenant_id
  );

  -- Return from deleting the workspace
  RETURN QUERY
  DELETE FROM workspaces w
  WHERE w.workspace_id = in_workspace_id
    AND w.tenant_id = in_tenant_id
  RETURNING w.workspace_id, w.variant_id;
END;
$$;

CREATE OR REPLACE FUNCTION commit_workspace_directories (
  w_collections_dir_id UUID,
  w_parameters_dir_id UUID,
  w_values_dir_id UUID,
  w_variant_id UUID,
  in_tenant_id VARCHAR
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  old_collections_dir_id UUID;
  old_parameters_dir_id UUID;
  old_values_dir_id UUID;
BEGIN
  -- Reassign workspace directories to version 1
  UPDATE collections_directory
  SET version_num = 1, workspace_id = NULL
  WHERE directory_id = w_collections_dir_id AND tenant_id = in_tenant_id;

  UPDATE parameters_directory
  SET version_num = 1, workspace_id = NULL
  WHERE directory_id = w_parameters_dir_id AND tenant_id = in_tenant_id;

  UPDATE values_directory
  SET version_num = 1, workspace_id = NULL
  WHERE directory_id = w_values_dir_id AND tenant_id = in_tenant_id;

  -- Save old version directory IDs before overwriting
  SELECT v.collections_directory, v.parameters_directory, v.values_directory
  INTO STRICT old_collections_dir_id, old_parameters_dir_id, old_values_dir_id
  FROM versions v
  WHERE v.version_num = 1
    AND v.variant_id = w_variant_id
    AND v.tenant_id = in_tenant_id;

  -- Overwrite version with workspace directories
  UPDATE versions
  SET collections_directory = w_collections_dir_id,
      parameters_directory = w_parameters_dir_id,
      values_directory = w_values_dir_id
  WHERE versions.version_num = 1
    AND versions.variant_id = w_variant_id
    AND versions.tenant_id = in_tenant_id;

  -- Delete old unused version directories
  DELETE FROM collections_directory cd
  WHERE cd.directory_id = old_collections_dir_id AND cd.tenant_id = in_tenant_id;

  DELETE FROM parameters_directory pd
  WHERE pd.directory_id = old_parameters_dir_id AND pd.tenant_id = in_tenant_id;

  DELETE FROM values_directory vd
  WHERE vd.directory_id = old_values_dir_id AND vd.tenant_id = in_tenant_id;
END;
$$;

GRANT ALL PRIVILEGES ON TABLE
	tenants,
	projects,
	catalogs,
	variants,
	versions,
	version_sequences,
  workspaces,
  catalog_objects,
  collections_directory,
  parameters_directory,
  values_directory,
  namespaces,
  views
TO catalogrw;
