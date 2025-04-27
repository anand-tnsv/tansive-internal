package db

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWorkspace(t *testing.T) {
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

	// Create a version 1 for the variant
	version := models.Version{
		VariantID: variant.VariantID,
		TenantID:  tenantID,
		Info:      info,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID)

	// Test case: Successfully create a workspace
	workspace := models.Workspace{
		Label:       "workspace1",
		Description: "First workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &workspace)
	assert.NoError(t, err)
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	// Verify that the workspace was created successfully
	retrievedWorkspace, err := DB(ctx).GetWorkspace(ctx, workspace.WorkspaceID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedWorkspace)
	assert.Equal(t, "workspace1", retrievedWorkspace.Label)

	// Get the catalog given a workpace
	catalogForWorkspace, err := DB(ctx).GetCatalogForWorkspace(ctx, workspace.WorkspaceID)
	assert.NoError(t, err)
	assert.Equal(t, catalog.CatalogID, catalogForWorkspace.CatalogID)
	assert.Equal(t, info, catalogForWorkspace.Info)

	// Test case: Create a workspace with invalid label (should fail due to check constraint)
	invalidLabelWorkspace := models.Workspace{
		Label:       "invalid label with spaces",
		Description: "This workspace should fail",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &invalidLabelWorkspace)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test case: Create a workspace with invalid variant ID (should fail)
	invalidVariantIDWorkspace := models.Workspace{
		Label:       "workspace3",
		Description: "This workspace should fail due to invalid variant ID",
		Info:        info,
		BaseVersion: 1,
		VariantID:   uuid.New(),
	}
	err = DB(ctx).CreateWorkspace(ctx, &invalidVariantIDWorkspace)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidVariant)

	// Test case: Create a duplicate workspace (should fail due to unique constraint when label is non-empty)
	duplicateWorkspace := models.Workspace{
		Label:       "workspace1", // same label as the first created workspace
		Description: "This workspace should fail due to duplicate label",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &duplicateWorkspace)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrAlreadyExists)

	// Test case: Missing tenant ID in context (should fail)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).CreateWorkspace(ctxWithoutTenant, &workspace)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestDeleteWorkspace(t *testing.T) {
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

	// Create a version 1 for the variant
	version := models.Version{
		VariantID: variant.VariantID,
		TenantID:  tenantID,
		Info:      info,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID)

	// Test case: Successfully create and then delete a workspace
	workspace := models.Workspace{
		Label:       "workspace1",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &workspace)
	assert.NoError(t, err)

	// Verify the workspace was created successfully
	retrievedWorkspace, err := DB(ctx).GetWorkspace(ctx, workspace.WorkspaceID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedWorkspace)

	// Delete the workspace
	err = DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)
	assert.NoError(t, err)

	// Verify that the workspace was deleted
	_, err = DB(ctx).GetWorkspace(ctx, workspace.WorkspaceID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Attempt to delete a non-existent workspace (should fail gracefully)
	err = DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID) // Attempt to delete again
	assert.NoError(t, err)                                    // Should not return an error, as it may be idempotent

	// Test case: Missing tenant ID in context (should fail)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).DeleteWorkspace(ctxWithoutTenant, workspace.WorkspaceID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestGetWorkspace(t *testing.T) {
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

	// Create a version 1 for the variant
	version := models.Version{
		VariantID: variant.VariantID,
		TenantID:  tenantID,
		Info:      info,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID)

	// Test case: Successfully create and retrieve a workspace
	workspace := models.Workspace{
		Label:       "workspace1",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &workspace)
	assert.NoError(t, err)
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	// Retrieve the workspace and verify its properties
	retrievedWorkspace, err := DB(ctx).GetWorkspace(ctx, workspace.WorkspaceID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedWorkspace)
	assert.Equal(t, workspace.WorkspaceID, retrievedWorkspace.WorkspaceID)
	assert.Equal(t, "workspace1", retrievedWorkspace.Label)
	assert.Equal(t, "A test workspace", retrievedWorkspace.Description)
	assert.Equal(t, info, retrievedWorkspace.Info)

	// Test case: Attempt to retrieve a non-existent workspace (should fail)
	nonExistentWorkspaceID := uuid.New()
	_, err = DB(ctx).GetWorkspace(ctx, nonExistentWorkspaceID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Missing tenant ID in context (should fail)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	_, err = DB(ctx).GetWorkspace(ctxWithoutTenant, workspace.WorkspaceID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestUpdateWorkspaceLabel(t *testing.T) {
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

	// Create a version 1 for the variant
	version := models.Version{
		VariantID: variant.VariantID,
		TenantID:  tenantID,
		Info:      info,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID)

	// Test case: Successfully create and update the label of a workspace
	workspace := models.Workspace{
		Label:       "original_label",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &workspace)
	assert.NoError(t, err)
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	// Update the workspace label
	newLabel := "updated_label"
	err = DB(ctx).UpdateWorkspaceLabel(ctx, workspace.WorkspaceID, newLabel)
	assert.NoError(t, err)

	// Verify that the label was updated successfully
	updatedWorkspace, err := DB(ctx).GetWorkspace(ctx, workspace.WorkspaceID)
	assert.NoError(t, err)
	assert.Equal(t, newLabel, updatedWorkspace.Label)

	// Test case: Attempt to update with an invalid label format (should fail)
	invalidLabel := "invalid label with spaces"
	err = DB(ctx).UpdateWorkspaceLabel(ctx, workspace.WorkspaceID, invalidLabel)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test case: Create another workspace with a unique label
	duplicateWorkspace := models.Workspace{
		Label:       "unique_label",
		Description: "Another workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	err = DB(ctx).CreateWorkspace(ctx, &duplicateWorkspace)
	assert.NoError(t, err)
	defer DB(ctx).DeleteWorkspace(ctx, duplicateWorkspace.WorkspaceID)

	// Attempt to update the first workspace to use the duplicate label (should fail)
	err = DB(ctx).UpdateWorkspaceLabel(ctx, workspace.WorkspaceID, "unique_label")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrAlreadyExists)

	// Test case: Attempt to update a non-existent workspace (should fail)
	nonExistentWorkspaceID := uuid.New()
	err = DB(ctx).UpdateWorkspaceLabel(ctx, nonExistentWorkspaceID, "new_label")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Missing tenant ID in context (should fail)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).UpdateWorkspaceLabel(ctxWithoutTenant, workspace.WorkspaceID, "another_label")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestCreateCollection(t *testing.T) {
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	assert.NoError(t, DB(ctx).CreateTenant(ctx, tenantID))
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	assert.NoError(t, DB(ctx).CreateProject(ctx, projectID))
	defer DB(ctx).DeleteProject(ctx, projectID)

	var info pgtype.JSONB
	assert.NoError(t, info.Set(`{"key": "value"}`))

	catalog := models.Catalog{
		Name:        "test_catalog_wc",
		Description: "test catalog for workspace collection",
		Info:        info,
	}
	assert.NoError(t, DB(ctx).CreateCatalog(ctx, &catalog))
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	variant := models.Variant{
		Name:        "test_variant_wc",
		Description: "test variant for workspace collection",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	assert.NoError(t, DB(ctx).CreateVariant(ctx, &variant))
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	workspace := models.Workspace{
		Label:       "original_label",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	assert.NoError(t, DB(ctx).CreateWorkspace(ctx, &workspace))
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	// Test case: Create workspace collection using default namespace
	wc := models.Collection{
		CollectionID:     uuid.New(),
		Path:             "/config/db",
		Hash:             "a3f1f81c9d26b37286f0828b8fecd851e35b0e7dfc51c58c9fd1a038d451de56",
		CollectionSchema: "/DbSchema",
		RepoID:           workspace.WorkspaceID,
		VariantID:        variant.VariantID,
	}
	err := DB(ctx).UpsertCollection(ctx, &wc, workspace.ValuesDir)
	require.NoError(t, err)

	// keep this wc
	wc_keep := wc

	// Test case: Duplicate collection (same path, namespace, etc.)
	err = DB(ctx).UpsertCollection(ctx, &wc, workspace.ValuesDir)
	require.NoError(t, err)

	//check containment of collection schema
	b, err := DB(ctx).HasReferencesToCollectionSchema(ctx, wc.CollectionSchema, workspace.ValuesDir)
	require.NoError(t, err)
	assert.True(t, b)
	b, err = DB(ctx).HasReferencesToCollectionSchema(ctx, "/some/random/schema", workspace.ValuesDir)
	require.NoError(t, err)
	assert.False(t, b)

	// Test case: Invalid path format
	wc.Path = "invalid path"
	wc.CollectionID = uuid.New()
	err = DB(ctx).UpsertCollection(ctx, &wc, workspace.ValuesDir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test case: Missing tenant ID
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).UpsertCollection(ctxWithoutTenant, &wc, workspace.ValuesDir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrMissingTenantID)

	err = DB(ctx).UpsertCollection(ctx, &wc_keep, workspace.ValuesDir)
	assert.NoError(t, err)
	// Delete the collection
	_, err = DB(ctx).DeleteCollection(ctx, wc_keep.Path, workspace.ValuesDir)
	assert.NoError(t, err)
}

func TestGetCollection(t *testing.T) {
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	assert.NoError(t, DB(ctx).CreateTenant(ctx, tenantID))
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	assert.NoError(t, DB(ctx).CreateProject(ctx, projectID))
	defer DB(ctx).DeleteProject(ctx, projectID)

	var info pgtype.JSONB
	assert.NoError(t, info.Set(`{"key": "value"}`))

	catalog := models.Catalog{Name: "get_test_catalog", Info: info}
	assert.NoError(t, DB(ctx).CreateCatalog(ctx, &catalog))
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	variant := models.Variant{Name: "get_test_variant", CatalogID: catalog.CatalogID, Info: info}
	assert.NoError(t, DB(ctx).CreateVariant(ctx, &variant))
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	workspace := models.Workspace{
		Label:       "original_label",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	assert.NoError(t, DB(ctx).CreateWorkspace(ctx, &workspace))
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	wc := models.Collection{
		CollectionID:     uuid.New(),
		Path:             "/get/example",
		Hash:             "abcd1234",
		CollectionSchema: "/BasicSchema",
		RepoID:           workspace.WorkspaceID,
		VariantID:        variant.VariantID,
	}
	assert.NoError(t, DB(ctx).UpsertCollection(ctx, &wc, workspace.ValuesDir))

	// Valid get
	result, err := DB(ctx).GetCollection(ctx, wc.Path, workspace.ValuesDir)
	require.NoError(t, err)
	assert.Equal(t, wc.Path, result.Path)

	// Not found
	_, err = DB(ctx).GetCollection(ctx, "/missing/path", workspace.ValuesDir)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}

func TestUpdateCollection(t *testing.T) {
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	assert.NoError(t, DB(ctx).CreateTenant(ctx, tenantID))
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	assert.NoError(t, DB(ctx).CreateProject(ctx, projectID))
	defer DB(ctx).DeleteProject(ctx, projectID)

	var info pgtype.JSONB
	assert.NoError(t, info.Set(`{"key": "value"}`))

	catalog := models.Catalog{Name: "update_test_catalog", Info: info}
	assert.NoError(t, DB(ctx).CreateCatalog(ctx, &catalog))
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	variant := models.Variant{Name: "update_test_variant", CatalogID: catalog.CatalogID, Info: info}
	assert.NoError(t, DB(ctx).CreateVariant(ctx, &variant))
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	workspace := models.Workspace{
		Label:       "original_label",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	assert.NoError(t, DB(ctx).CreateWorkspace(ctx, &workspace))
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	wc := models.Collection{
		CollectionID:     uuid.New(),
		Path:             "/update/example",
		Hash:             schemastore.HexEncodedSHA512([]byte("original_hash")),
		CollectionSchema: "/UpdateSchema",
		RepoID:           workspace.WorkspaceID,
		VariantID:        variant.VariantID,
	}
	assert.NoError(t, DB(ctx).UpsertCollection(ctx, &wc, workspace.ValuesDir))

	// Update
	wc.Hash = schemastore.HexEncodedSHA512([]byte("updated_hash"))
	require.NoError(t, DB(ctx).UpdateCollection(ctx, &wc, workspace.ValuesDir))

	// Verify
	got, err := DB(ctx).GetCollection(ctx, wc.Path, workspace.ValuesDir)
	assert.NoError(t, err)
	assert.Equal(t, wc.Hash, got.Hash)

	// Not found
	wc.Path = "/not/found"
	err = DB(ctx).UpdateCollection(ctx, &wc, workspace.ValuesDir)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}

func TestDeleteCollection(t *testing.T) {
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	assert.NoError(t, DB(ctx).CreateTenant(ctx, tenantID))
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	assert.NoError(t, DB(ctx).CreateProject(ctx, projectID))
	defer DB(ctx).DeleteProject(ctx, projectID)

	var info pgtype.JSONB
	assert.NoError(t, info.Set(`{"key": "value"}`))

	catalog := models.Catalog{Name: "delete_test_catalog", Info: info}
	assert.NoError(t, DB(ctx).CreateCatalog(ctx, &catalog))
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	variant := models.Variant{Name: "delete_test_variant", CatalogID: catalog.CatalogID, Info: info}
	assert.NoError(t, DB(ctx).CreateVariant(ctx, &variant))
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	workspace := models.Workspace{
		Label:       "original_label",
		Description: "A test workspace",
		Info:        info,
		BaseVersion: 1,
		VariantID:   variant.VariantID,
	}
	assert.NoError(t, DB(ctx).CreateWorkspace(ctx, &workspace))
	defer DB(ctx).DeleteWorkspace(ctx, workspace.WorkspaceID)

	wc := models.Collection{
		CollectionID:     uuid.New(),
		Path:             "/delete/example",
		Hash:             "xyz123",
		CollectionSchema: "/DeleteSchema",
		RepoID:           workspace.WorkspaceID,
		VariantID:        variant.VariantID,
	}
	assert.NoError(t, DB(ctx).UpsertCollection(ctx, &wc, workspace.ValuesDir))

	// Delete
	_, err := DB(ctx).DeleteCollection(ctx, wc.Path, workspace.ValuesDir)
	assert.NoError(t, err)

	// Confirm
	_, err = DB(ctx).GetCollection(ctx, wc.Path, workspace.ValuesDir)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Delete again
	_, err = DB(ctx).DeleteCollection(ctx, wc.Path, workspace.ValuesDir)
	assert.NoError(t, err) // should not error
}
