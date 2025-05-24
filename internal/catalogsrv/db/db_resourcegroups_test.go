package db

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestResourceGroupOperations(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

	var info pgtype.JSONB
	err = info.Set(`{"key": "value"}`)
	assert.NoError(t, err)

	// Create the catalog for testing
	catalog := models.Catalog{
		Name:        "test_catalog",
		Description: "A test catalog",
		Info:        info,
	}
	err = DB(ctx).CreateCatalog(ctx, &catalog)
	assert.NoError(t, err)
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Create a mock resource group
	rg := &models.ResourceGroup{
		Path:      "/test/resourcegroup",
		Hash:      "test_hash_123",
		VariantID: variant.VariantID,
	}

	// Create a mock catalog object
	obj := &models.CatalogObject{
		Hash:     rg.Hash,
		Type:     types.CatalogObjectTypeResourceGroup,
		Version:  "v1",
		TenantID: tenantID,
		Data:     []byte(`{"key": "value"}`),
	}

	// Test UpsertResourceGroupObject
	err = DB(ctx).UpsertResourceGroupObject(ctx, rg, obj, variant.ResourceGroupsDirectoryID)
	require.NoError(t, err)

	// Test GetResourceGroup
	retrievedRG, err := DB(ctx).GetResourceGroup(ctx, rg.Path, variant.VariantID, variant.ResourceGroupsDirectoryID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedRG)
	assert.Equal(t, rg.Path, retrievedRG.Path)
	assert.Equal(t, rg.Hash, strings.TrimSpace(retrievedRG.Hash))
	assert.Equal(t, rg.VariantID, retrievedRG.VariantID)

	// Test GetResourceGroupObject
	retrievedObj, err := DB(ctx).GetResourceGroupObject(ctx, rg.Path, variant.ResourceGroupsDirectoryID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedObj)
	assert.Equal(t, obj.Hash, strings.TrimSpace(retrievedObj.Hash))
	assert.Equal(t, obj.Type, retrievedObj.Type)
	assert.Equal(t, obj.Version, retrievedObj.Version)

	// Test UpdateResourceGroup
	rg.Hash = "updated_hash_456"
	err = DB(ctx).UpdateResourceGroup(ctx, rg, variant.ResourceGroupsDirectoryID)
	assert.NoError(t, err)

	// Verify update
	updatedRG, err := DB(ctx).GetResourceGroup(ctx, rg.Path, variant.VariantID, variant.ResourceGroupsDirectoryID)
	assert.NoError(t, err)
	assert.NotNil(t, updatedRG)
	assert.Equal(t, rg.Hash, strings.TrimSpace(updatedRG.Hash))

	// Test DeleteResourceGroup
	deletedHash, err := DB(ctx).DeleteResourceGroup(ctx, rg.Path, variant.ResourceGroupsDirectoryID)
	assert.NoError(t, err)
	assert.Equal(t, rg.Hash, deletedHash)

	// Verify deletion
	_, err = DB(ctx).GetResourceGroup(ctx, rg.Path, variant.VariantID, variant.ResourceGroupsDirectoryID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test error cases
	// Test with invalid directory ID
	_, err = DB(ctx).GetResourceGroup(ctx, rg.Path, variant.VariantID, uuid.Nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test with missing tenant ID
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	_, err = DB(ctx).GetResourceGroup(ctxWithoutTenant, rg.Path, variant.VariantID, variant.ResourceGroupsDirectoryID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrMissingTenantID)
}
