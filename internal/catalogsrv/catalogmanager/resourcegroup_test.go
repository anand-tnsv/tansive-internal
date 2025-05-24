package catalogmanager

import (
	"context"
	"path"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestResourceGroupNewManager(t *testing.T) {
	tests := []struct {
		name          string
		jsonInput     string
		metadata      *schemamanager.SchemaMetadata
		expectedError bool
	}{
		{
			name: "valid resource group",
			jsonInput: `{
				"version": "v1",
				"kind": "ResourceGroup",
				"metadata": {
					"name": "test-group",
					"catalog": "test-catalog",
					"namespace": "default",
					"variant": "default"
				},
				"spec": {
					"resources": {
						"resource1": {
							"schema": {"type": "integer"},
							"value": 42
						}
					}
				}
			}`,
			metadata: &schemamanager.SchemaMetadata{
				Name:      "test-group",
				Catalog:   "test-catalog",
				Namespace: types.NullableStringFrom("default"),
				Variant:   types.NullableStringFrom("default"),
			},
			expectedError: false,
		},
		{
			name:          "empty json",
			jsonInput:     "",
			metadata:      nil,
			expectedError: true,
		},
		{
			name: "invalid json",
			jsonInput: `{
				"version": "v1",
				"kind": "InvalidKind",
				"metadata": {
					"name": "test-group",
					"catalog": "test-catalog",
					"namespace": "default",
					"variant": "default"
				}
			}`,
			metadata: &schemamanager.SchemaMetadata{
				Name:      "test-group",
				Catalog:   "test-catalog",
				Namespace: types.NullableStringFrom("default"),
				Variant:   types.NullableStringFrom("default"),
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewResourceGroupManager(context.Background(), []byte(tt.jsonInput), tt.metadata)
			if tt.expectedError {
				assert.Error(t, err)
				assert.Nil(t, manager)
			} else {
				if err != nil {
					t.Logf("Unexpected error: %v", err)
				}
				assert.NoError(t, err)
				assert.NotNil(t, manager)
				if manager != nil {
					// Test Metadata() method
					metadata := manager.Metadata()
					assert.Equal(t, tt.metadata.Name, metadata.Name)
					assert.Equal(t, tt.metadata.Catalog, metadata.Catalog)
					assert.Equal(t, tt.metadata.Namespace, metadata.Namespace)
					assert.Equal(t, tt.metadata.Variant, metadata.Variant)

					// Test FullyQualifiedName() method
					expectedName := path.Clean(tt.metadata.Path + "/" + tt.metadata.Name)
					assert.Equal(t, expectedName, manager.FullyQualifiedName())
				}
			}
		})
	}
}

func TestLoadResourceGroupManagerByPath(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	require.NoError(t, err)
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	err = db.DB(ctx).CreateProject(ctx, projectID)
	require.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	var info pgtype.JSONB
	err = info.Set(`{"key": "value"}`)
	require.NoError(t, err)

	// Create the catalog for testing
	catalog := models.Catalog{
		Name:        "test-catalog",
		Description: "A test catalog",
		Info:        info,
	}
	err = db.DB(ctx).CreateCatalog(ctx, &catalog)
	require.NoError(t, err)
	defer db.DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Set catalog ID in context
	ctx = common.SetCatalogIdInContext(ctx, catalog.CatalogID)

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test-variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = db.DB(ctx).CreateVariant(ctx, &variant)
	require.NoError(t, err)
	defer db.DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Set variant ID and name in context
	ctx = common.SetVariantIdInContext(ctx, variant.VariantID)
	ctx = common.SetVariantInContext(ctx, variant.Name)

	t.Run("Load existing resource group", func(t *testing.T) {
		// Create a resource group
		rgJson := []byte(`{
			"version": "v1",
			"kind": "ResourceGroup",
			"metadata": {
				"name": "test-rg",
				"description": "Test resource group",
				"catalog": "test-catalog",
				"variant": "test-variant",
				"path": "/test"
			},
			"spec": {
				"resources": {
					"resource1": {
						"schema": {
							"type": "object",
							"properties": {
								"name": {
									"type": "string"
								}
							}
						},
						"value": {
							"name": "test"
						}
					}
				}
			}
		}`)

		// Create and save the resource group
		rgm, err := NewResourceGroupManager(ctx, rgJson, nil)
		require.NoError(t, err)
		err = rgm.Save(ctx)
		require.NoError(t, err)

		// Create metadata for loading
		metadata := &schemamanager.SchemaMetadata{
			Name:    "test-rg",
			Catalog: "test-catalog",
			Variant: types.NullableStringFrom("test-variant"),
			Path:    "/test",
		}

		// Load the resource group by path
		loadedRgm, err := LoadResourceGroupManagerByPath(ctx, metadata)
		require.NoError(t, err)
		require.NotNil(t, loadedRgm)

		// Verify metadata
		loadedMetadata := loadedRgm.Metadata()
		assert.Equal(t, "test-rg", loadedMetadata.Name)
		assert.Equal(t, "test-catalog", loadedMetadata.Catalog)
		assert.Equal(t, "test-variant", loadedMetadata.Variant.String())
		assert.Equal(t, "/test", loadedMetadata.Path)

		// Verify resources
		value, err := loadedRgm.GetValue(ctx, "resource1")
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"name": "test"}, value.Get())

		// Verify storage path
		assert.Equal(t, rgm.GetStoragePath(), loadedRgm.GetStoragePath())
	})

	t.Run("Load non-existent resource group", func(t *testing.T) {
		// Create metadata for non-existent resource group
		metadata := &schemamanager.SchemaMetadata{
			Name:    "non-existent",
			Catalog: "test-catalog",
			Variant: types.NullableStringFrom("test-variant"),
			Path:    "/test",
		}

		// Try to load a non-existent resource group
		loadedRgm, err := LoadResourceGroupManagerByPath(ctx, metadata)
		assert.Error(t, err)
		assert.Nil(t, loadedRgm)
	})

	t.Run("Load resource group with invalid variant", func(t *testing.T) {
		// Create a resource group
		rgJson := []byte(`{
			"version": "v1",
			"kind": "ResourceGroup",
			"metadata": {
				"name": "test-rg2",
				"description": "Test resource group 2",
				"catalog": "test-catalog",
				"variant": "test-variant",
				"path": "/test"
			},
			"spec": {
				"resources": {
					"resource1": {
						"schema": {
							"type": "object",
							"properties": {
								"name": {
									"type": "string"
								}
							}
						},
						"value": {
							"name": "test"
						}
					}
				}
			}
		}`)

		// Create and save the resource group
		rgm, err := NewResourceGroupManager(ctx, rgJson, nil)
		require.NoError(t, err)
		err = rgm.Save(ctx)
		require.NoError(t, err)

		// Create metadata with invalid variant
		metadata := &schemamanager.SchemaMetadata{
			Name:    "test-rg2",
			Catalog: "test-catalog",
			Variant: types.NullableStringFrom("invalid-variant"),
			Path:    "/test",
		}

		// Try to load with invalid variant
		loadedRgm, err := LoadResourceGroupManagerByPath(ctx, metadata)
		assert.Error(t, err)
		assert.Nil(t, loadedRgm)
	})
}
