package postgresql

import (
	"context"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

func (om *objectManager) UpsertResourceGroup(ctx context.Context, rg *models.ResourceGroup, directoryID uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	rg.TenantID = tenantID

	if directoryID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("invalid directory ID")
	}

	err := om.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeResourceGroup,
		directoryID,
		rg.Path,
		models.ObjectRef{
			Hash: rg.Hash,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (om *objectManager) GetResourceGroup(ctx context.Context, path string, variantID uuid.UUID, directoryID uuid.UUID) (*models.ResourceGroup, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	if directoryID == uuid.Nil {
		return nil, dberror.ErrInvalidInput.Msg("invalid directory ID")
	}

	objRef, err := om.GetObjectRefByPath(ctx, types.CatalogObjectTypeResourceGroup, directoryID, path)
	if err != nil {
		return nil, err
	}

	return &models.ResourceGroup{
		Path:      path,
		Hash:      objRef.Hash,
		VariantID: variantID,
		TenantID:  tenantID,
	}, nil
}

func (om *objectManager) GetResourceGroupObject(ctx context.Context, path string, directoryID uuid.UUID) (*models.CatalogObject, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	if directoryID == uuid.Nil {
		return nil, dberror.ErrInvalidInput.Msg("invalid directory ID")
	}

	return om.LoadObjectByPath(ctx, types.CatalogObjectTypeResourceGroup, directoryID, path)
}

func (om *objectManager) UpdateResourceGroup(ctx context.Context, rg *models.ResourceGroup, directoryID uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if directoryID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("invalid directory ID")
	}

	objRef, err := om.GetObjectRefByPath(ctx, types.CatalogObjectTypeResourceGroup, directoryID, rg.Path)
	if err != nil {
		return err
	}
	objRef.Hash = rg.Hash
	err = om.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeResourceGroup,
		directoryID,
		rg.Path,
		*objRef,
	)
	if err != nil {
		return err
	}
	return nil
}

func (om *objectManager) DeleteResourceGroup(ctx context.Context, path string, directoryID uuid.UUID) (string, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", dberror.ErrMissingTenantID
	}

	if directoryID == uuid.Nil {
		return "", dberror.ErrInvalidInput.Msg("invalid directory ID")
	}

	deletedHash, err := om.DeleteObjectByPath(ctx, types.CatalogObjectTypeResourceGroup, directoryID, path)
	if err != nil {
		return "", err
	}

	return string(deletedHash), nil
}

func (om *objectManager) UpsertResourceGroupObject(ctx context.Context, rg *models.ResourceGroup, obj *models.CatalogObject, directoryID uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	rg.TenantID = tenantID

	if directoryID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("invalid directory ID")
	}

	// First create/update the catalog object
	err := om.CreateCatalogObject(ctx, obj)
	if err != nil {
		// If the object already exists, that's fine - we can continue
		if !err.Is(dberror.ErrAlreadyExists) {
			return err
		}
	}

	// Then add/update the directory entry
	err = om.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeResourceGroup,
		directoryID,
		rg.Path,
		models.ObjectRef{
			Hash: rg.Hash,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
