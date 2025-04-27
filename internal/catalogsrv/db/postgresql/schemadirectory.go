package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"path"
	"regexp"
	"strings"

	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
)

func (om *objectManager) CreateSchemaDirectory(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if dir.DirectoryID == uuid.Nil {
		dir.DirectoryID = uuid.New()
	}
	if dir.VariantID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("variant_id cannot be empty")
	}
	if dir.TenantID == "" {
		return dberror.ErrInvalidInput.Msg("tenant_id cannot be empty")
	}
	if len(dir.Directory) == 0 {
		return dberror.ErrInvalidInput.Msg("directory cannot be nil")
	}

	dir.TenantID = tenantID

	tx, err := om.conn().BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to start transaction")
		return dberror.ErrDatabase.Err(err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	errDb := om.createSchemaDirectoryWithTransaction(ctx, t, dir, tx)
	if errDb != nil {
		tx.Rollback()
		return errDb
	}

	if err := tx.Commit(); err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	return nil
}

func (om *objectManager) SetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID, dir []byte) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		UPDATE ` + tableName + `
		SET directory = $1
		WHERE directory_id = $2 AND tenant_id = $3;`

	_, err := om.conn().ExecContext(ctx, query, dir, id, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (om *objectManager) GetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID) ([]byte, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		SELECT directory
		FROM ` + tableName + `
		WHERE directory_id = $1 AND tenant_id = $2;`

	var dir []byte
	err := om.conn().QueryRowContext(ctx, query, id, tenantID).Scan(&dir)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("directory not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}
	return dir, nil
}

func (om *objectManager) createSchemaDirectoryWithTransaction(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory, tx *sql.Tx) apperrors.Error {
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}
	if dir.DirectoryID == uuid.Nil {
		dir.DirectoryID = uuid.New()
	}
	var refName string
	var refId any
	if dir.WorkspaceID != uuid.Nil {
		refName = "workspace_id"
		refId = dir.WorkspaceID
	} else if dir.VersionNum != 0 {
		refName = "version_num"
		refId = dir.VersionNum
	} else {
		return dberror.ErrInvalidInput.Msg("either workspace_id or version_num must be set")
	}

	// Insert the schema directory into the database and get created uuid
	query := ` INSERT INTO ` + tableName + ` (directory_id, ` + refName + `, variant_id, tenant_id, directory)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (directory_id, tenant_id) DO NOTHING RETURNING directory_id;`

	var directoryID uuid.UUID
	err := tx.QueryRowContext(ctx, query, dir.DirectoryID, refId, dir.VariantID, dir.TenantID, dir.Directory).Scan(&directoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return dberror.ErrAlreadyExists.Msg("schema directory already exists")
		} else {
			return dberror.ErrDatabase.Err(err)
		}
	}

	dir.DirectoryID = directoryID

	return nil
}

func (om *objectManager) GetSchemaDirectory(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID) (*models.SchemaDirectory, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `SELECT directory_id, variant_id, tenant_id, directory
		FROM ` + tableName + `
		WHERE directory_id = $1 AND tenant_id = $2;`

	dir := &models.SchemaDirectory{}
	err := om.conn().QueryRowContext(ctx, query, directoryID, tenantID).Scan(&dir.DirectoryID, &dir.VariantID, &dir.TenantID, &dir.Directory)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("schema directory not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}
	return dir, nil
}

func (om *objectManager) GetObjectRefByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (*models.ObjectRef, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		SELECT directory-> $1 AS object
		FROM ` + tableName + `
		WHERE directory_id = $2 AND tenant_id = $3;`

	var objectData []byte
	err := om.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&objectData)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("object not found in directory")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	if len(objectData) == 0 {
		return nil, dberror.ErrNotFound.Msg("object not found in directory")
	}

	var obj models.ObjectRef
	if err := json.Unmarshal(objectData, &obj); err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &obj, nil
}

func (om *objectManager) LoadObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (*models.CatalogObject, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	log.Ctx(ctx).Debug().Str("path", path).Str("DirectoryID", directoryID.String()).Msg("Loading object by path")
	query := `
		WITH hash_cte AS (
			SELECT (directory-> $1 ->> 'hash') AS hash
			FROM ` + tableName + `
			WHERE directory_id = $2 AND tenant_id = $3
		)
		SELECT
			co.hash,
			co.type,
			co.version,
			co.tenant_id,
			co.data
		FROM
			hash_cte
		JOIN
			catalog_objects co
		ON
			hash_cte.hash = co.hash
		WHERE
			co.tenant_id = $3;
	`

	var hash, version string
	var objType types.CatalogObjectType
	var data []byte
	err := om.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&hash, &objType, &version, &tenantID, &data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("object not found in directory or catalog")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	// Create and populate the CatalogObject
	catalogObj := &models.CatalogObject{
		Hash:     hash,
		Type:     objType,
		Version:  version,
		TenantID: tenantID,
	}

	catalogObj.Data = data
	// Decompress the data
	if config.CompressCatalogObjects {
		catalogObj.Data, err = snappy.Decode(nil, data)
		if err != nil {
			return nil, dberror.ErrDatabase.Err(err)
		}
	}

	return catalogObj, nil
}

func (om *objectManager) AddOrUpdateObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, obj models.ObjectRef) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	if !isValidPath(path) {
		return dberror.ErrInvalidInput.Msg("invalid path")
	}

	// Convert the object to JSON
	data, err := json.Marshal(obj)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	query := `
		UPDATE ` + tableName + `
		SET directory = jsonb_set(directory, ARRAY[$1], $2::jsonb)
		WHERE directory_id = $3 AND tenant_id = $4;`

	result, err := om.conn().ExecContext(ctx, query, path, data, directoryID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		// No matching row was found with directory_id and tenant_id
		return dberror.ErrNotFound.Msg("object not found")
	}

	// get object to verify update
	if o, err := om.GetObjectRefByPath(ctx, t, directoryID, path); err != nil {
		return err
	} else if o.Hash != obj.Hash {
		return dberror.ErrDatabase.Msg("object hash mismatch after update")
	}

	return nil
}

func (om *objectManager) AddReferencesToObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, references models.References) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Convert references to JSONB array of objects
	referenceData, err := json.Marshal(references)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	query := `
		UPDATE ` + tableName + `
		SET directory = jsonb_set(
			directory,
			ARRAY[$1, 'references'],
			(
				SELECT jsonb_agg(x)
				FROM (
					SELECT DISTINCT x
					FROM (
						SELECT x
						FROM jsonb_array_elements(
							CASE
								WHEN jsonb_typeof(directory #> ARRAY[$1, 'references']) = 'array' THEN
									directory #> ARRAY[$1, 'references']
								ELSE
									'[]'::jsonb
							END
						) AS x
						WHERE x->>'name' NOT IN (
							SELECT value->>'name'
							FROM jsonb_array_elements($2::jsonb) AS value
						)
						UNION ALL
						SELECT value
						FROM jsonb_array_elements($2::jsonb) AS value
					) AS combined
				) AS deduplicated
			),
			true
		)
		WHERE directory_id = $3 AND tenant_id = $4;`

	// Execute the query
	result, err := om.conn().ExecContext(ctx, query, path, referenceData, directoryID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("object not found")
	}

	return nil
}

func (om *objectManager) GetAllReferences(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (models.References, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Query to extract the references array
	query := `
		SELECT COALESCE(directory #> ARRAY[$1, 'references'], '[]'::jsonb)
		FROM ` + tableName + `
		WHERE directory_id = $2 AND tenant_id = $3;`

	var jsonbData []byte
	err := om.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&jsonbData)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("object not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	// Unmarshal the JSONB data into a slice of Reference structs
	var references models.References
	if err := json.Unmarshal(jsonbData, &references); err != nil {
		return nil, dberror.ErrDatabase.Msg("failed to unmarshal references").Err(err)
	}

	return references, nil
}

func (om *objectManager) DeleteReferenceFromObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, refName string) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// SQL query to remove a specific reference by parameter from the JSONB array
	query := `
		UPDATE ` + tableName + `
		SET directory = jsonb_set(
			directory,
			ARRAY[$1, 'references'],
			COALESCE(
				(
					SELECT jsonb_agg(x)
					FROM jsonb_array_elements(
						CASE
							WHEN jsonb_typeof(directory #> ARRAY[$1, 'references']) = 'array' THEN
								directory #> ARRAY[$1, 'references']
							ELSE
								'[]'::jsonb
						END
					) x
					WHERE x->>'name' != $2
				),
				'[]'::jsonb
			),
			true
		)
		WHERE directory_id = $3 AND tenant_id = $4;`

	// Execute the query
	result, err := om.conn().ExecContext(ctx, query, path, refName, directoryID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("object not found")
	}

	return nil
}

func (om *objectManager) DeleteObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (types.Hash, apperrors.Error) {
	var hash types.Hash = ""
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return hash, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return hash, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}
	log.Ctx(ctx).Debug().Str("path", path).Str("DirectoryID", directoryID.String()).Msg("Deleting object by path")
	// Update and return whether the key was removed
	query := `
		WITH to_delete AS (
			SELECT directory -> $1 ->> 'hash' AS deleted_hash
			FROM ` + tableName + `
			WHERE directory_id = $2 AND tenant_id = $3 AND directory ? $1
		)
		UPDATE ` + tableName + `
		SET directory = directory - $1
		WHERE directory_id = $2 AND tenant_id = $3 AND directory ? $1
		RETURNING (SELECT deleted_hash FROM to_delete);

	`
	var result sql.NullString
	err := om.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&result)
	if err == sql.ErrNoRows {
		return hash, nil // Key did not exist, so nothing was removed
	} else if err != nil {
		return hash, dberror.ErrDatabase.Err(err)
	} else if !result.Valid {
		return hash, dberror.ErrNotFound.Msg("object not found")
	}
	hash = types.Hash(result.String)

	return hash, nil
}

func (om *objectManager) UpdateObjectHashForPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, hash string) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		UPDATE ` + tableName + `
		SET directory = jsonb_set(
			directory,
			ARRAY[$1, 'hash'],
			to_jsonb($2::TEXT)
		)
		WHERE directory_id = $3 AND tenant_id = $4;`

	result, err := om.conn().ExecContext(ctx, query, path, hash, directoryID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("object not found")
	}

	return nil
}

func (om *objectManager) DeleteTree(ctx context.Context, directoryIds models.DirectoryIDs, path string) ([]string, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	var paramDirID, collectionDirID uuid.UUID
	for _, dirId := range directoryIds {
		if dirId.Type == types.CatalogObjectTypeCollectionSchema {
			collectionDirID = dirId.ID
		} else if dirId.Type == types.CatalogObjectTypeParameterSchema {
			paramDirID = dirId.ID
		}
	}
	if collectionDirID == uuid.Nil || paramDirID == uuid.Nil {
		return nil, dberror.ErrInvalidInput.Msg("invalid directory ids")
	}

	tx, err := om.conn().BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to start transaction")
		return nil, dberror.ErrDatabase.Err(err)
	}
	defer func() {
		tx.Rollback()
	}()

	// get the collection directory
	query := `SELECT directory FROM collections_directory WHERE directory_id = $1 AND tenant_id = $2 FOR UPDATE;`
	var b []byte
	err = tx.QueryRowContext(ctx, query, collectionDirID, tenantID).Scan(&b)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("collection directory not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to get collection directory")
		return nil, dberror.ErrDatabase.Err(err)
	}
	collectionDir, err := models.JSONToDirectory(b)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection directory")
		return nil, dberror.ErrDatabase.Err(err)
	}
	// get the parameter directory
	query = `SELECT directory FROM parameters_directory WHERE directory_id = $1 AND tenant_id = $2 FOR UPDATE;`
	b = nil
	err = tx.QueryRowContext(ctx, query, paramDirID, tenantID).Scan(&b)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("parameter directory not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to get parameter directory")
		return nil, dberror.ErrDatabase.Err(err)
	}
	parameterDir, err := models.JSONToDirectory(b)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal parameter directory")
		return nil, dberror.ErrDatabase.Err(err)
	}
	// define a function to delete paths
	deletePaths := func(t types.CatalogObjectType, dir models.Directory, path string) ([]string, apperrors.Error) {
		/*
			We remove all objects that start with the path
			We also remove all references that start with the path
		*/
		var removedObjects []string
		for p, objRef := range dir {
			if strings.HasPrefix(p, path) {
				removedObjects = append(removedObjects, objRef.Hash)
				delete(dir, p)
			} else {
				newRefs := []models.Reference{}
				for _, ref := range objRef.References {
					if !strings.HasPrefix(ref.Name, path) {
						newRefs = append(newRefs, ref)
					}
				}
				objRef.References = newRefs
				dir[p] = objRef
			}
		}
		b, err = models.DirectoryToJSON(dir)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal collection directory")
			return nil, dberror.ErrDatabase.Err(err)
		}
		var query string
		if t == types.CatalogObjectTypeCollectionSchema {
			query = `UPDATE collections_directory SET directory = $1 WHERE directory_id = $2 AND tenant_id = $3;`
			_, err = tx.ExecContext(ctx, query, b, collectionDirID, tenantID)
		} else {
			query = `UPDATE parameters_directory SET directory = $1 WHERE directory_id = $2 AND tenant_id = $3;`
			_, err = tx.ExecContext(ctx, query, b, paramDirID, tenantID)
		}
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to update collection directory")
			return nil, dberror.ErrDatabase.Err(err)
		}
		return removedObjects, nil
	}

	// delete the paths
	var removedObjects []string
	if objects, err := deletePaths(types.CatalogObjectTypeCollectionSchema, collectionDir, path); err != nil {
		return nil, err
	} else {
		removedObjects = objects
	}
	if objects, err := deletePaths(types.CatalogObjectTypeParameterSchema, parameterDir, path); err != nil {
		return nil, err
	} else {
		removedObjects = append(removedObjects, objects...)
	}

	// commit the transaction
	if err := tx.Commit(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to commit transaction")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return removedObjects, nil
}

type directoryObjDeleteOptions struct {
	ignoreReferences              bool
	replaceReferencesWithAncestor bool
	deleteReferences              bool
}

func (o *directoryObjDeleteOptions) IgnoreReferences(b bool) {
	o.ignoreReferences = b
}

func (o *directoryObjDeleteOptions) ReplaceReferencesWithAncestor(b bool) {
	o.replaceReferencesWithAncestor = b
}

func (o *directoryObjDeleteOptions) DeleteReferences(b bool) {
	o.deleteReferences = b
}

func (o *directoryObjDeleteOptions) ApplyOptions(opts ...models.DirectoryObjectDeleteOptions) {
	for _, opt := range opts {
		opt(o)
	}
}

func (om *objectManager) DeleteObjectWithReferences(ctx context.Context,
	t types.CatalogObjectType,
	dirIDs models.DirectoryIDs,
	delPath string,
	opts ...models.DirectoryObjectDeleteOptions) (objHash string, errRet apperrors.Error) {

	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", dberror.ErrMissingTenantID
	}

	o := &directoryObjDeleteOptions{}
	o.ApplyOptions(opts...)

	var paramDirID, collectionDirID uuid.UUID
	for _, dirId := range dirIDs {
		if dirId.Type == types.CatalogObjectTypeCollectionSchema {
			collectionDirID = dirId.ID
		} else if dirId.Type == types.CatalogObjectTypeParameterSchema {
			paramDirID = dirId.ID
		}
	}
	if collectionDirID == uuid.Nil || paramDirID == uuid.Nil {
		return "", dberror.ErrInvalidInput.Msg("invalid directory ids")
	}

	objName := path.Base(delPath)
	objPath := path.Dir(delPath)
	if objPath == "." {
		objPath = "/"
	}
	if objName == "" {
		return "", dberror.ErrInvalidInput.Msg("invalid path")
	}

	var (
		deleteDirTableName string
		deleteDirID        uuid.UUID
		refDirTableName    string
		refDirID           uuid.UUID
		ancestorPath       string
	)
	if t == types.CatalogObjectTypeCollectionSchema {
		deleteDirTableName = "collections_directory"
		deleteDirID = collectionDirID
		refDirTableName = "parameters_directory"
		refDirID = paramDirID
	} else if t == types.CatalogObjectTypeParameterSchema {
		deleteDirTableName = "parameters_directory"
		deleteDirID = paramDirID
		refDirTableName = "collections_directory"
		refDirID = collectionDirID
	} else {
		return "", dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	tx, err := om.conn().BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to start transaction")
		return "", dberror.ErrDatabase.Err(err)
	}
	defer func() {
		// recover from panic
		if r := recover(); r != nil {
			tx.Rollback()
			log.Ctx(ctx).Error().Err(r.(error)).Msg("panic in DeleteObjectWithReferences")
			// raise the panic back
			panic(r)
		} else {
			if errRet != nil {
				objHash = ""
				tx.Rollback()
			} else {
				tx.Commit()
			}
		}

	}()

	writeDirectory := func(tableName string, dirID uuid.UUID, dir models.Directory) apperrors.Error {
		b, err := models.DirectoryToJSON(dir)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal directory")
			return dberror.ErrDatabase.Err(err)
		}
		query := `UPDATE ` + tableName + ` SET directory = $1 WHERE directory_id = $2 AND tenant_id = $3;`
		_, err = tx.ExecContext(ctx, query, b, dirID, tenantID)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to update directory")
			return dberror.ErrDatabase.Err(err)
		}
		return nil
	}

	// Fetch the directory to delete from
	query := `SELECT directory FROM ` + deleteDirTableName + ` WHERE directory_id = $1 AND tenant_id = $2 FOR UPDATE;`
	var b []byte
	err = tx.QueryRowContext(ctx, query, deleteDirID, tenantID).Scan(&b)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", dberror.ErrNotFound.Msg("directory not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to get directory")
		return "", dberror.ErrDatabase.Err(err)
	}
	deleteDir, err := models.JSONToDirectory(b)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal directory")
		return "", dberror.ErrDatabase.Err(err)
	}
	// find the object and its references
	var objRef models.ObjectRef
	var ok bool
	if objRef, ok = deleteDir[delPath]; !ok {
		return "", dberror.ErrNotFound.Msg("object not found")
	}

	if len(objRef.References) == 0 || o.ignoreReferences {
		// if there are no references to this object, just remove the object
		delete(deleteDir, delPath)
		return objRef.Hash, writeDirectory(deleteDirTableName, deleteDirID, deleteDir)
	}

	if o.replaceReferencesWithAncestor {
		// get the ancestor
		ancestorPathDir := ""
		for p := range deleteDir {
			if p == delPath {
				continue
			}
			if strings.HasSuffix(p, objName) {
				d := path.Dir(p)
				if d == "." {
					d = "/"
				}
				if strings.HasPrefix(objPath, d) && strings.HasPrefix(d, ancestorPathDir) {
					ancestorPathDir = d
				}
			}
		}
		if len(ancestorPathDir) == 0 {
			return "", dberror.ErrNoAncestorReferencesFound
		}
		ancestorPath = path.Clean(ancestorPathDir + "/" + objName)
	}

	if o.replaceReferencesWithAncestor || o.deleteReferences {
		// get the references directory
		query = `SELECT directory FROM ` + refDirTableName + ` WHERE directory_id = $1 AND tenant_id = $2 FOR UPDATE;`
		b = nil
		err = tx.QueryRowContext(ctx, query, refDirID, tenantID).Scan(&b)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", dberror.ErrNotFound.Msg("directory not found")
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to get directory")
			return "", dberror.ErrDatabase.Err(err)
		}
		refDir, err := models.JSONToDirectory(b)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal directory")
			return "", dberror.ErrDatabase.Err(err)
		}
		// remove the references to the object we need to delete
		for _, r := range objRef.References {
			if obj, ok := refDir[r.Name]; ok {
				var newRefs models.References
				for _, ref := range obj.References {
					if ref.Name != delPath {
						newRefs = append(newRefs, ref)
					} else if o.replaceReferencesWithAncestor {
						if !newRefs.Contains(ancestorPath) {
							newRefs = append(newRefs, models.Reference{
								Name: ancestorPath,
							})
						}
					}
				}
				obj.References = newRefs
				refDir[r.Name] = obj
			}
		}
		if err := writeDirectory(refDirTableName, refDirID, refDir); err != nil {
			return "", err
		}
		// delete the object
		delete(deleteDir, delPath)
		if err := writeDirectory(deleteDirTableName, deleteDirID, deleteDir); err != nil {
			return "", err
		}
		return objRef.Hash, nil
	}

	return "", nil
}

// FindClosestObject searches for an object in a JSONB directory that is associated with the specified targetName
// and located at the closest matching path to the provided startPatom. It traverses outward from the startPath
// to the nearest parent paths until it finds a matcom.
//
// Parameters:
// - ctx: The context for handling deadlines, cancellation signals, and other request-scoped values.
// - t: The type of catalog object, used to identify the correct table within the schema.
// - directoryID: The unique identifier of the directory in which to search for the targetName.
// - targetName: The specific key name to search for within paths in the JSONB directory.
// - startPath: The initial path from which the search begins, traversing outward to locate the closest matcom.
//
// Returns:
// - string: The path in the directory that is closest to startPath and contains targetName as the last path segment.
// - map[string]any: The object associated with the closest matching patom.
// - apperrors.Error: Error, if any occurs during execution.
//
// How It Works:
// 1. Constructs a LIKE pattern using targetName to match paths that end with "/<targetName>" in the JSONB directory.
// 2. Queries the directory for all paths ending in the specified targetName and orders them by path length in descending order.
// 3. For each matching path, it checks if the path is either equal to or a parent path of the startPatom.
// 4. Returns the first match (closest path) and its associated object, if found.
//
// Example:
// Assume the directory JSON contains paths like "/a/b/c/d" and "/a/d":
//
//     path, object, err := om.FindClosestObject(ctx, catalogType, directoryID, "d", "/a/b/c")
//     This returns path="/a/b/c/d" and the object associated with "/a/b/c/d"
//
// If no path with the specified targetName is found within or above startPath, the function returns an empty string and nil for the object.

func (om *objectManager) FindClosestObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, targetName, startPath string) (string, *models.ObjectRef, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return "", nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Build LIKE pattern for paths ending with "/<targetName>"
	likePattern := "%" + "/" + targetName

	// SQL to find paths ending in targetName, ordered by path length descending
	query := `
SELECT key AS path, value AS object
FROM ` + tableName + `, LATERAL jsonb_each_text(directory)
WHERE directory_id = $1 AND tenant_id = $2
  AND key LIKE $3
ORDER BY LENGTH(key) DESC;
`

	rows, err := om.conn().QueryContext(ctx, query, directoryID, tenantID, likePattern)
	if err != nil {
		return "", nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var closestPath string
	var closestObject models.ObjectRef

	for rows.Next() {
		var delPath string
		var objectData []byte

		if err := rows.Scan(&delPath, &objectData); err != nil {
			return "", nil, dberror.ErrDatabase.Err(err)
		}

		// Check if the path is equal to or a parent of startPath
		if isParentPath(delPath, startPath, targetName) {
			closestPath = delPath

			if err := json.Unmarshal(objectData, &closestObject); err != nil {
				return "", nil, dberror.ErrDatabase.Err(err)
			}
			break
		}
	}

	// Error handling for row scan
	if err := rows.Err(); err != nil {
		return "", nil, dberror.ErrDatabase.Err(err)
	}

	if closestPath == "" {
		return "", nil, nil
	}

	return closestPath, &closestObject, nil
}

// isParentPath checks if path is a parent of startPatom.
func isParentPath(path, startPath, targetName string) bool {
	parentPath := strings.TrimSuffix(path, "/"+targetName)
	b := strings.HasPrefix(startPath, parentPath)
	if b {
		r := strings.TrimPrefix(startPath, parentPath)
		return r == "" || strings.HasPrefix(r, "/")
	}
	return false
}

func (om *objectManager) PathExists(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (bool, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return false, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return false, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		SELECT directory ? $1 AS exists
		FROM ` + tableName + `
		WHERE directory_id = $2 AND tenant_id = $3;`

	var exists bool
	err := om.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&exists)
	if err != nil {
		return false, dberror.ErrDatabase.Err(err)
	}

	return exists, nil
}

func getSchemaDirectoryTableName(t types.CatalogObjectType) string {
	switch t {
	case types.CatalogObjectTypeCollectionSchema:
		return "collections_directory"
	case types.CatalogObjectTypeParameterSchema:
		return "parameters_directory"
	case types.CatalogObjectTypeCatalogCollection:
		return "values_directory"
	default:
		return ""
	}
}

func isValidPath(path string) bool {
	var validPathPattern = regexp.MustCompile(`^(/[A-Za-z0-9_-]+)+$`)
	return validPathPattern.MatchString(path)
}
