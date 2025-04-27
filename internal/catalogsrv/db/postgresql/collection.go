package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
)

// The Collections interface functions are a shim on top of schema directory.  This would allow for a different implementation
// in future, if necessary.

func (om *objectManager) UpsertCollection(ctx context.Context, c *models.Collection, dir uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	c.TenantID = tenantID

	if !isValidPath(c.CollectionSchema) {
		return dberror.ErrInvalidInput.Msg("invalid collection schema")
	}

	err := om.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeCatalogCollection,
		dir,
		c.Path,
		models.ObjectRef{
			Hash:       c.Hash,
			BaseSchema: c.CollectionSchema,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (om *objectManager) GetCollection(ctx context.Context, path string, dir uuid.UUID) (*models.Collection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	objRef, err := om.GetObjectRefByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, path)
	if err != nil {
		return nil, err
	}

	return &models.Collection{
		Path:             path,
		Hash:             objRef.Hash,
		CollectionSchema: objRef.BaseSchema,
	}, nil
}

func (om *objectManager) GetCollectionObject(ctx context.Context, path string, dir uuid.UUID) (*models.CatalogObject, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	return om.LoadObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, path)
}

func (om *objectManager) UpdateCollection(ctx context.Context, c *models.Collection, dir uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if !isValidPath(c.CollectionSchema) {
		return dberror.ErrInvalidInput.Msg("invalid collection schema")
	}

	objRef, err := om.GetObjectRefByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, c.Path)
	if err != nil {
		return err
	}
	objRef.Hash = c.Hash
	objRef.BaseSchema = c.CollectionSchema
	err = om.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeCatalogCollection,
		dir,
		c.Path,
		*objRef,
	)
	if err != nil {
		return err
	}
	return nil
}

func (om *objectManager) DeleteCollection(ctx context.Context, path string, dir uuid.UUID) (string, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", dberror.ErrMissingTenantID
	}

	deletedHash, err := om.DeleteObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, path)
	if err != nil {
		return "", err
	}

	return string(deletedHash), nil
}

func (om *objectManager) HasReferencesToCollectionSchema(ctx context.Context, collectionSchema string, dir uuid.UUID) (bool, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return false, dberror.ErrMissingTenantID
	}

	query := `
		SELECT 1
		FROM values_directory
		WHERE jsonb_path_query_array(directory, '$.*.base_schema') @> to_jsonb($1::text)
		AND directory_id = $2
		AND tenant_id = $3
		LIMIT 1;
	`
	var exists bool // we'll probably just hit the ErrNoRows case in case of false
	dberr := om.conn().QueryRowContext(ctx, query, collectionSchema, dir, tenantID).Scan(&exists)
	if dberr != nil {
		if dberr == sql.ErrNoRows {
			return false, nil
		}
		return false, dberror.ErrDatabase.Err(dberr)
	}
	return exists, nil
}

/*
func (om *objectManager) getValuesDirectory(ctx context.Context, repoId, variantId uuid.UUID) (uuid.UUID, apperrors.Error) {
	var dir uuid.UUID
	if repoId != variantId {
		w, err := om.m.GetWorkspace(ctx, repoId)
		if err != nil {
			return uuid.Nil, err
		}
		dir = w.ValuesDir
	} else {
		v, err := om.m.GetVersion(ctx, 1, variantId)
		if err != nil {
			return uuid.Nil, dberror.ErrDatabase.Err(err)
		}
		dir = v.ValuesDir
	}
	return dir, nil
}
*/
