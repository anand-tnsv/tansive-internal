package db

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgtype"
	json "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

func TestSkillSetOperations(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

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
	require.NoError(t, err)
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

	// Create a mock skillset
	ss := &models.SkillSet{
		Path:      "/test/skillset",
		Hash:      "test_hash_123",
		VariantID: variant.VariantID,
		Metadata:  json.RawMessage(`{"description": "test skillset"}`),
	}

	// Create a mock catalog object
	obj := &models.CatalogObject{
		Hash:     ss.Hash,
		Type:     catcommon.CatalogObjectTypeSkillset,
		Version:  "v1",
		TenantID: tenantID,
		Data:     []byte(`{"key": "value"}`),
	}

	// Test UpsertSkillSetObject
	err = DB(ctx).UpsertSkillSetObject(ctx, ss, obj, variant.SkillsetDirectoryID)
	require.NoError(t, err)

	// Test GetSkillSet
	retrievedSS, err := DB(ctx).GetSkillSet(ctx, ss.Path, variant.VariantID, variant.SkillsetDirectoryID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedSS)
	assert.Equal(t, ss.Path, retrievedSS.Path)
	assert.Equal(t, ss.Hash, strings.TrimSpace(retrievedSS.Hash))
	assert.Equal(t, ss.VariantID, retrievedSS.VariantID)
	assert.Equal(t, ss.Metadata, retrievedSS.Metadata)

	// Test GetSkillSetObject
	retrievedObj, err := DB(ctx).GetSkillSetObject(ctx, ss.Path, variant.SkillsetDirectoryID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedObj)
	assert.Equal(t, obj.Hash, strings.TrimSpace(retrievedObj.Hash))
	assert.Equal(t, obj.Type, retrievedObj.Type)
	assert.Equal(t, obj.Version, retrievedObj.Version)

	// Test UpdateSkillSet
	ss.Hash = "updated_hash_456"
	ss.Metadata = json.RawMessage(`{"description": "updated skillset"}`)
	err = DB(ctx).UpdateSkillSet(ctx, ss, variant.SkillsetDirectoryID)
	assert.NoError(t, err)

	// Verify update
	updatedSS, err := DB(ctx).GetSkillSet(ctx, ss.Path, variant.VariantID, variant.SkillsetDirectoryID)
	assert.NoError(t, err)
	assert.NotNil(t, updatedSS)
	assert.Equal(t, ss.Hash, strings.TrimSpace(updatedSS.Hash))
	assert.Equal(t, ss.Metadata, updatedSS.Metadata)

	// Test ListSkillSets
	skillsets, err := DB(ctx).ListSkillSets(ctx, variant.SkillsetDirectoryID)
	assert.NoError(t, err)
	assert.Len(t, skillsets, 1)
	assert.Equal(t, ss.Path, skillsets[0].Path)
	assert.Equal(t, ss.Hash, strings.TrimSpace(skillsets[0].Hash))
	assert.Equal(t, ss.Metadata, skillsets[0].Metadata)

	// Test DeleteSkillSet
	deletedHash, err := DB(ctx).DeleteSkillSet(ctx, ss.Path, variant.SkillsetDirectoryID)
	assert.NoError(t, err)
	assert.Equal(t, ss.Hash, deletedHash)

	// Verify deletion
	_, err = DB(ctx).GetSkillSet(ctx, ss.Path, variant.VariantID, variant.SkillsetDirectoryID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test error cases
	// Test with invalid directory ID
	_, err = DB(ctx).GetSkillSet(ctx, ss.Path, variant.VariantID, uuid.Nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test with missing tenant ID
	ctxWithoutTenant := catcommon.WithTenantID(ctx, "")
	_, err = DB(ctx).GetSkillSet(ctxWithoutTenant, ss.Path, variant.VariantID, variant.SkillsetDirectoryID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrMissingTenantID)
}
