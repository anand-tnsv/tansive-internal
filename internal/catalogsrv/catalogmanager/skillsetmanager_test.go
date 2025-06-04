package catalogmanager

import (
	"errors"
	"path"
	"strings"
	"testing"

	"github.com/jackc/pgtype"
	json "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestSkillSetValidation(t *testing.T) {
	tests := []struct {
		name          string
		jsonInput     string
		expectedError bool
		errorTypes    []string // List of expected error messages
	}{
		{
			name: "valid skillset with provider",
			jsonInput: `{
				"version": "v1",
				"kind": "SkillSet",
				"metadata": {
					"name": "test-skillset",
					"catalog": "test-catalog",
					"namespace": "default",
					"variant": "default",
					"path": "/skillsets/test-skillset"
				},
				"spec": {
					"version": "1.0.0",
					"runner": {
						"id": "system.commandrunner",
						"config": {
							"command": "python3 test.py"
						}
					},
					"context": [
						{
							"name": "test-context",
							"provider": {
								"id": "system.redis",
								"config": {
									"host": "localhost"
								}
							},
							"schema": {"type": "object"}
						}
					],
					"skills": [
						{
							"name": "test-skill",
							"description": "A test skill",
							"inputSchema": {"type": "object"},
							"outputSchema": {"type": "object"},
							"exportedActions": ["test.action"]
						}
					],
					"dependencies": [
						{
							"path": "/resources/test",
							"kind": "Resource",
							"alias": "test-resource",
							"actions": ["read"]
						}
					]
				}
			}`,
			expectedError: false,
		},
		{
			name: "invalid kind",
			jsonInput: `{
				"version": "v1",
				"kind": "InvalidKind",
				"metadata": {
					"name": "test-skillset",
					"catalog": "test-catalog",
					"namespace": "default",
					"variant": "default",
					"path": "/skillsets/test-skillset"
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"unsupported kind"},
		},
		{
			name: "missing required fields",
			jsonInput: `{
				"version": "v1",
				"kind": "SkillSet"
			}`,
			expectedError: true,
			errorTypes:    []string{"SkillSet.Metadata: missing required attribute"},
		},
		{
			name: "invalid skillset name format",
			jsonInput: `{
				"version": "v1",
				"kind": "SkillSet",
				"metadata": {
					"name": "invalid name",
					"catalog": "test-catalog",
					"namespace": "default",
					"variant": "default",
					"path": "/skillsets/invalid-name"
				},
				"spec": {
					"version": "1.0.0",
					"provider": {
						"id": "system.commandrunner",
						"config": {
							"command": "python3 test.py"
						}
					},
					"context": [
						{
							"name": "test-context",
							"provider": {
								"id": "system.redis",
								"config": {
									"host": "localhost"
								}
							},
							"schema": {"type": "object"}
						}
					],
					"skills": [
						{
							"name": "test-skill",
							"description": "A test skill",
							"inputSchema": {"type": "object"},
							"outputSchema": {"type": "object"},
							"exportedActions": ["test.action"]
						}
					],
					"dependencies": [
						{
							"path": "/resources/test",
							"kind": "Resource",
							"alias": "test-resource",
							"actions": ["read"]
						}
					]
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"invalid name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s SkillSet
			err := json.Unmarshal([]byte(tt.jsonInput), &s)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			validationErrors := s.Validate()
			if tt.expectedError {
				assert.NotEmpty(t, validationErrors, "Expected validation errors but got none")
				if len(tt.errorTypes) > 0 {
					errorMessages := make([]string, len(validationErrors))
					for i, err := range validationErrors {
						errorMessages[i] = err.Error()
					}
					for _, expectedErr := range tt.errorTypes {
						found := false
						for _, actualErr := range errorMessages {
							if strings.Contains(actualErr, expectedErr) {
								found = true
								break
							}
						}
						assert.True(t, found, "Expected error message containing '%s' not found in validation errors: %v", expectedErr, errorMessages)
					}
				}
			} else {
				assert.Empty(t, validationErrors, "Unexpected validation errors: %v", validationErrors)
			}
		})
	}
}

func TestSkillSetManagerSave(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

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
	ctx = catcommon.WithCatalogID(ctx, catalog.CatalogID)

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test-variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = db.DB(ctx).CreateVariant(ctx, &variant)
	require.NoError(t, err)

	// Create a valid skillset
	validJSON := `{
		"version": "v1",
		"kind": "SkillSet",
		"metadata": {
			"name": "test-skillset",
			"catalog": "test-catalog",
			"namespace": "default",
			"variant": "test-variant",
			"path": "/skillsets"
		},
		"spec": {
			"version": "1.0.0",
			"runner": {
				"id": "system.commandrunner",
				"config": {
					"command": "python3 test.py"
				}
			},
			"context": [
				{
					"name": "test-context",
					"provider": {
						"id": "system.redis",
						"config": {
							"host": "localhost"
						}
					}
				}
			],
			"skills": [
				{
					"name": "test-skill",
					"description": "A test skill",
					"inputSchema": {"type": "object"},
					"outputSchema": {"type": "object"},
					"exportedActions": ["test.action"]
				}
			],
			"dependencies": [
				{
					"path": "/resources/test",
					"kind": "Resource",
					"alias": "test-resource",
					"actions": ["read"]
				}
			]
		}
	}`

	manager := &skillSetManager{}
	err = json.Unmarshal([]byte(validJSON), &manager.skillSet)
	require.NoError(t, err)

	metadata := manager.skillSet.Metadata

	storagePath := metadata.GetStoragePath(catcommon.CatalogObjectTypeSkillset) + "/" + metadata.Name

	t.Run("Save - valid skillset", func(t *testing.T) {
		err := manager.Save(ctx)
		assert.NoError(t, err)

		// Verify the skillset was saved
		_, err = db.DB(ctx).GetSkillSet(ctx, storagePath, variant.VariantID, variant.SkillsetDirectoryID)
		assert.NoError(t, err)
	})

	t.Run("saves skillset with metadata", func(t *testing.T) {
		// ... existing test code until after save ...

		// Verify the skillset was saved with correct metadata
		ss, err := db.DB(ctx).GetSkillSet(ctx, storagePath, variant.VariantID, variant.SkillsetDirectoryID)
		require.NoError(t, err)
		require.NotNil(t, ss)
		require.NotEmpty(t, ss.Metadata)

		// Parse and verify metadata contents
		var metadata SkillMetadata
		require.NoError(t, json.Unmarshal(ss.Metadata, &metadata))

		// Verify skills in metadata
		require.Len(t, metadata.Skills, 1)
		require.Equal(t, "test-skill", metadata.Skills[0].Name)
		require.Equal(t, []policy.Action{"test.action"}, metadata.Skills[0].ExportedActions)

		// Verify dependencies in metadata
		require.Len(t, metadata.Dependencies, 1)
		require.Equal(t, "/resources/test", metadata.Dependencies[0].Path)
		require.Equal(t, KindResource, metadata.Dependencies[0].Kind)
		require.Equal(t, []policy.Action{"read"}, metadata.Dependencies[0].Actions)
	})

	t.Run("saves skillset with multiple skills in metadata", func(t *testing.T) {
		// Create a skillset with multiple skills
		ss := &SkillSet{
			Version: "v1",
			Kind:    "SkillSet",
			Metadata: interfaces.Metadata{
				Name:      "multi-skill-set",
				Namespace: types.NullableStringFrom("default"),
				Path:      "/skillsets/multi-skill-set",
				Catalog:   "test-catalog",
				Variant:   types.NullableStringFrom("test-variant"),
			},
			Spec: SkillSetSpec{
				Version: "1.0.0",
				Runner: SkillSetRunner{
					ID: "system.commandrunner",
					Config: map[string]any{
						"command": "python3 skillsets/multi-skill-set.py",
					},
				},
				Skills: []Skill{
					{
						Name:         "skill1",
						Description:  "First skill",
						InputSchema:  json.RawMessage(`{"type": "object"}`),
						OutputSchema: json.RawMessage(`{"type": "object"}`),
						ExportedActions: []policy.Action{
							"skill1.action1",
							"skill1.action2",
						},
					},
					{
						Name:         "skill2",
						Description:  "Second skill",
						InputSchema:  json.RawMessage(`{"type": "object"}`),
						OutputSchema: json.RawMessage(`{"type": "object"}`),
						ExportedActions: []policy.Action{
							"skill2.action1",
						},
					},
				},
				Dependencies: []Dependency{
					{
						Path:    "/resources/test-resource",
						Kind:    KindResource,
						Alias:   "test-resource",
						Actions: []policy.Action{"resource.action"},
					},
				},
			},
		}

		// Save the skillset
		sm := &skillSetManager{skillSet: *ss}
		err := sm.Save(ctx)
		require.NoError(t, err)

		// Verify the skillset was saved with correct metadata
		pathWithName := path.Clean(ss.Metadata.GetStoragePath(catcommon.CatalogObjectTypeSkillset) + "/" + ss.Metadata.Name)
		savedSS, err := db.DB(ctx).GetSkillSet(ctx, pathWithName, variant.VariantID, variant.SkillsetDirectoryID)
		require.NoError(t, err)
		require.NotNil(t, savedSS)
		require.NotEmpty(t, savedSS.Metadata)

		// Parse and verify metadata contents
		var metadata SkillMetadata
		require.NoError(t, json.Unmarshal(savedSS.Metadata, &metadata))

		// Verify skills in metadata
		require.Len(t, metadata.Skills, 2)

		// Verify first skill
		require.Equal(t, "skill1", metadata.Skills[0].Name)
		require.Equal(t, []policy.Action{"skill1.action1", "skill1.action2"}, metadata.Skills[0].ExportedActions)

		// Verify second skill
		require.Equal(t, "skill2", metadata.Skills[1].Name)
		require.Equal(t, []policy.Action{"skill2.action1"}, metadata.Skills[1].ExportedActions)

		// Verify dependencies
		require.Len(t, metadata.Dependencies, 1)
		require.Equal(t, "/resources/test-resource", metadata.Dependencies[0].Path)
		require.Equal(t, KindResource, metadata.Dependencies[0].Kind)
		require.Equal(t, []policy.Action{"resource.action"}, metadata.Dependencies[0].Actions)
	})
}

func TestSkillSetManagerDelete(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

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
	ctx = catcommon.WithCatalogID(ctx, catalog.CatalogID)

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test-variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = db.DB(ctx).CreateVariant(ctx, &variant)
	require.NoError(t, err)

	metadata := &interfaces.Metadata{
		Name:      "test-skillset",
		Catalog:   "test-catalog",
		Namespace: types.NullableStringFrom("default"),
		Variant:   types.NullableStringFrom("test-variant"),
	}

	t.Run("Delete - non-existent skillset", func(t *testing.T) {
		err := DeleteSkillSet(ctx, metadata)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrObjectNotFound))
	})

	t.Run("Delete - valid skillset", func(t *testing.T) {
		// First create a skillset
		validJSON := `{
			"version": "v1",
			"kind": "SkillSet",
			"metadata": {
				"name": "test-skillset",
				"catalog": "test-catalog",
				"namespace": "default",
				"variant": "test-variant"
			},
			"spec": {
				"version": "1.0.0",
				"runner": {
					"id": "system.commandrunner",
					"config": {
						"command": "python3 test.py"
					}
				},
				"context": [
					{
						"name": "test-context",
						"provider": {
							"id": "system.redis",
							"config": {
								"host": "localhost"
							}
						}
					}
				],
				"skills": [
					{
						"name": "test-skill",
						"description": "A test skill",
						"inputSchema": {"type": "object"},
						"outputSchema": {"type": "object"},
						"exportedActions": ["test.action"]
					}
				],
				"dependencies": [
					{
						"path": "/resources/test",
						"kind": "Resource",
						"alias": "test-resource",
						"actions": ["read"]
					}
				]
			}
		}`

		manager := &skillSetManager{}
		err = json.Unmarshal([]byte(validJSON), &manager.skillSet)
		require.NoError(t, err)

		err = manager.Save(ctx)
		require.NoError(t, err)

		// Now delete it
		err = DeleteSkillSet(ctx, metadata)
		assert.NoError(t, err)

		// Verify it's gone
		path := getSkillSetStoragePath(metadata)
		_, err = db.DB(ctx).GetSkillSet(ctx, path, variant.VariantID, variant.SkillsetDirectoryID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, dberror.ErrNotFound))
	})
}
