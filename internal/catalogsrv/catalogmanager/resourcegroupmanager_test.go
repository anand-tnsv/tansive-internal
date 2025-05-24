package catalogmanager

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgtype"
	json "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/datatypes" // Import to register data types
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestResourceGroupValidation(t *testing.T) {
	tests := []struct {
		name          string
		jsonInput     string
		expectedError bool
		errorTypes    []string // List of expected error messages
	}{
		{
			name: "valid resource group with schema and value",
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
							"value": 42,
							"annotations": {
								"key1": "value1",
								"mcp:description": "This is a test resource description"
							}
						}
					}
				}
			}`,
			expectedError: false,
		},
		{
			name: "valid resource group with schema, value and inherit policy",
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
							"value": 42,
							"policy": "inherit",
							"annotations": {
								"key1": "value1",
								"mcp:description": "This is a test resource description"
							}
						}
					}
				}
			}`,
			expectedError: false,
		},
		{
			name: "valid resource group with schema, value and override policy",
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
							"schema": {"type": "string"},
							"value": "test",
							"policy": "override",
							"annotations": {
								"key1": "value1"
							}
						}
					}
				}
			}`,
			expectedError: false,
		},
		{
			name: "valid resource group with schema, value and no policy",
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
							"schema": {"type": "boolean"},
							"value": true,
							"annotations": {
								"key1": "value1"
							}
						}
					}
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
					"name": "test-group",
					"catalog": "test-catalog",
					"namespace": "default",
					"variant": "default"
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"unsupported kind"},
		},
		{
			name: "missing required fields",
			jsonInput: `{
				"version": "v1",
				"kind": "ResourceGroup"
			}`,
			expectedError: true,
			errorTypes:    []string{"metadata: missing required attribute"},
		},
		{
			name: "invalid resource name format",
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
						"invalid name": {
							"schema": {"type": "integer"},
							"value": 42
						}
					}
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"invalid name format"},
		},
		{
			name: "empty resource group",
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
					"resources": {}
				}
			}`,
			expectedError: false,
		},
		{
			name: "invalid resource - missing both provider and schema/value",
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
							"annotations": {
								"key1": "value1"
							}
						}
					}
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"validation failed"},
		},
		{
			name: "invalid policy value",
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
							"value": 42,
							"policy": "invalid"
						}
					}
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"invalid schema"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rg ResourceGroup
			err := json.Unmarshal([]byte(tt.jsonInput), &rg)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			validationErrors := rg.Validate()
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

func TestResourceGroupSpecValidation(t *testing.T) {
	val, err := types.NullableAnyFrom(42)
	assert.NoError(t, err)

	strVal, err := types.NullableAnyFrom("test")
	assert.NoError(t, err)

	boolVal, err := types.NullableAnyFrom(true)
	assert.NoError(t, err)

	tests := []struct {
		name          string
		spec          ResourceGroupSpec
		expectedError bool
	}{
		{
			name: "valid spec with schema and value",
			spec: ResourceGroupSpec{
				Resources: map[string]Resource{
					"valid-resource": {
						Schema: json.RawMessage(`{"type": "integer"}`),
						Value:  val,
						Annotations: schemamanager.Annotations{
							"key": "value",
						},
					},
				},
			},
			expectedError: false,
		},
		{
			name: "valid spec with schema, value and inherit policy",
			spec: ResourceGroupSpec{
				Resources: map[string]Resource{
					"valid-resource": {
						Schema: json.RawMessage(`{"type": "integer"}`),
						Value:  val,
						Policy: "inherit",
						Annotations: schemamanager.Annotations{
							"key": "value",
						},
					},
				},
			},
			expectedError: false,
		},
		{
			name: "valid spec with schema, value and override policy",
			spec: ResourceGroupSpec{
				Resources: map[string]Resource{
					"valid-resource": {
						Schema: json.RawMessage(`{"type": "string"}`),
						Value:  strVal,
						Policy: "override",
						Annotations: schemamanager.Annotations{
							"key": "value",
						},
					},
				},
			},
			expectedError: false,
		},
		{
			name: "valid spec with schema, value and no policy",
			spec: ResourceGroupSpec{
				Resources: map[string]Resource{
					"valid-resource": {
						Schema: json.RawMessage(`{"type": "boolean"}`),
						Value:  boolVal,
						Annotations: schemamanager.Annotations{
							"key": "value",
						},
					},
				},
			},
			expectedError: false,
		},
		{
			name: "empty spec",
			spec: ResourceGroupSpec{
				Resources: map[string]Resource{},
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rg := ResourceGroup{
				Version: "v1",
				Kind:    types.ResourceGroupKind,
				Metadata: schemamanager.SchemaMetadata{
					Name:      "test-group",
					Catalog:   "test-catalog",
					Namespace: types.NullableStringFrom("default"),
					Variant:   types.NullableStringFrom("default"),
				},
				Spec: tt.spec,
			}

			validationErrors := rg.Validate()
			if tt.expectedError {
				assert.NotEmpty(t, validationErrors, "Expected validation errors but got none")
			} else {
				assert.Empty(t, validationErrors, "Unexpected validation errors: %v", validationErrors)
			}
		})
	}
}

func TestResourceGroupSchemaValidation(t *testing.T) {
	tests := []struct {
		name          string
		jsonInput     string
		expectedError bool
		errorTypes    []string
	}{
		{
			name: "valid JSON schema",
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
							"schema": {"type": "object", "properties": {"name": {"type": "string"}}},
							"value": {"name": "test"},
							"annotations": {
								"key1": "value1"
							}
						}
					}
				}
			}`,
			expectedError: false,
		},
		{
			name: "invalid JSON schema syntax",
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
							"schema": {
								"type": "object",
								"properties": {
									"name": {
										"type": "invalid_type"
									}
								}
							},
							"value": {"name": "test"},
							"annotations": {
								"key1": "value1"
							}
						}
					}
				}
			}`,
			expectedError: true,
			errorTypes:    []string{"failed to compile schema"},
		},
		{
			name: "self-referential schema",
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
							"schema": {"$id": "inline://schema", "type": "object", "properties": {"name": {"type": "string"}}},
							"value": {"name": "test"},
							"annotations": {
								"key1": "value1"
							}
						}
					}
				}
			}`,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rg ResourceGroup
			err := json.Unmarshal([]byte(tt.jsonInput), &rg)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			validationErrors := rg.Validate()
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

func TestValidateValue(t *testing.T) {
	tests := []struct {
		name          string
		schema        string
		value         any
		expectedError bool
		errorTypes    []string
	}{
		{
			name:          "simple string validation",
			schema:        `{"type": "string"}`,
			value:         "test",
			expectedError: false,
		},
		{
			name:          "simple number validation",
			schema:        `{"type": "number"}`,
			value:         42.5,
			expectedError: false,
		},
		{
			name:          "simple integer validation",
			schema:        `{"type": "integer"}`,
			value:         42,
			expectedError: false,
		},
		{
			name:          "simple boolean validation",
			schema:        `{"type": "boolean"}`,
			value:         true,
			expectedError: false,
		},
		{
			name:          "simple object validation",
			schema:        `{"type": "object", "properties": {"name": {"type": "string"}}}`,
			value:         map[string]any{"name": "test"},
			expectedError: false,
		},
		{
			name:          "simple array validation",
			schema:        `{"type": "array", "items": {"type": "string"}}`,
			value:         []any{"test1", "test2"},
			expectedError: false,
		},
		{
			name:          "invalid type",
			schema:        `{"type": "string"}`,
			value:         42,
			expectedError: true,
			errorTypes:    []string{"expected string"},
		},
		{
			name:          "required property missing",
			schema:        `{"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}}`,
			value:         map[string]any{},
			expectedError: true,
			errorTypes:    []string{"missing properties"},
		},
		{
			name:          "self-referential schema",
			schema:        `{"$id": "inline://schema", "type": "object", "properties": {"name": {"type": "string"}, "children": {"$ref": "inline://schema"}}}`,
			value:         map[string]any{"name": "parent", "children": map[string]any{"name": "child"}},
			expectedError: false,
		},
		{
			name:          "if-then-else validation",
			schema:        `{"type": "object", "if": {"properties": {"type": {"const": "admin"}}}, "then": {"required": ["permissions"]}, "else": {"required": ["role"]}}`,
			value:         map[string]any{"type": "admin", "permissions": []string{"read", "write"}},
			expectedError: false,
		},
		{
			name:          "if-then-else validation failure",
			schema:        `{"type": "object", "if": {"properties": {"type": {"const": "admin"}}}, "then": {"required": ["permissions"]}, "else": {"required": ["role"]}}`,
			value:         map[string]any{"type": "admin"},
			expectedError: true,
			errorTypes:    []string{"missing properties"},
		},
		{
			name:          "dependent schemas",
			schema:        `{"type": "object", "properties": {"hasAddress": {"type": "boolean"}, "address": {"type": "string"}}, "dependentSchemas": {"hasAddress": {"required": ["address"]}}}`,
			value:         map[string]any{"hasAddress": true, "address": "123 Main St"},
			expectedError: false,
		},
		{
			name:          "dependent schemas failure",
			schema:        `{"type": "object", "properties": {"hasAddress": {"type": "boolean"}, "address": {"type": "string"}}, "dependentSchemas": {"hasAddress": {"required": ["address"]}}}`,
			value:         map[string]any{"hasAddress": true},
			expectedError: true,
			errorTypes:    []string{"missing properties"},
		},
		{
			name:          "array with tuple validation",
			schema:        `{"type": "array", "prefixItems": [{"type": "string"}, {"type": "number"}]}`,
			value:         []any{"test", 42},
			expectedError: false,
		},
		{
			name:          "array with tuple validation failure",
			schema:        `{"type": "array", "prefixItems": [{"type": "string"}, {"type": "number"}]}`,
			value:         []any{"test", "invalid"},
			expectedError: true,
			errorTypes:    []string{"expected number"},
		},
		{
			name:          "null value",
			schema:        `{"type": "null"}`,
			value:         nil,
			expectedError: false,
		},
		{
			name:          "null value failure",
			schema:        `{"type": "null"}`,
			value:         "not null",
			expectedError: true,
			errorTypes:    []string{"expected null"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource := Resource{
				Schema: json.RawMessage(tt.schema),
			}
			nv, err := types.NullableAnyFrom(tt.value)
			require.NoError(t, err)
			err = resource.ValidateValue(nv)
			if tt.expectedError {
				assert.Error(t, err)
				if len(tt.errorTypes) > 0 {
					errorMsg := err.Error()
					found := false
					for _, expectedErr := range tt.errorTypes {
						if strings.Contains(errorMsg, expectedErr) {
							found = true
							break
						}
					}
					assert.True(t, found, "Expected error message containing one of %v, got: %s", tt.errorTypes, errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestResourceGroupManagerValueOperations(t *testing.T) {
	validJSON := `{
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
				},
				"resource2": {
					"schema": {"type": "string"},
					"value": "test"
				}
			}
		}
	}`

	metadata := &schemamanager.SchemaMetadata{
		Name:      "test-group",
		Catalog:   "test-catalog",
		Namespace: types.NullableStringFrom("default"),
		Variant:   types.NullableStringFrom("default"),
	}

	manager, err := NewResourceGroupManager(context.Background(), []byte(validJSON), metadata)
	require.NoError(t, err)
	require.NotNil(t, manager)

	t.Run("GetValue - existing resource", func(t *testing.T) {
		value, err := manager.GetValue(context.Background(), "resource1")
		assert.NoError(t, err)
		assert.Equal(t, float64(42), value.Get()) // JSON numbers are unmarshaled as float64
	})

	t.Run("GetValue - non-existent resource", func(t *testing.T) {
		value, err := manager.GetValue(context.Background(), "nonexistent")
		assert.Error(t, err)
		assert.True(t, value.IsNil())
	})

	t.Run("GetValueJSON - existing resource", func(t *testing.T) {
		json, err := manager.GetValueJSON(context.Background(), "resource1")
		assert.NoError(t, err)
		assert.Equal(t, "42", string(json))
	})

	t.Run("GetValueJSON - non-existent resource", func(t *testing.T) {
		json, err := manager.GetValueJSON(context.Background(), "nonexistent")
		assert.Error(t, err)
		assert.Nil(t, json)
	})

	t.Run("SetValue - valid value", func(t *testing.T) {
		newValue, err := types.NullableAnyFrom(100)
		require.NoError(t, err)
		err = manager.SetValue(context.Background(), "resource1", newValue)
		assert.NoError(t, err)

		value, err := manager.GetValue(context.Background(), "resource1")
		assert.NoError(t, err)
		assert.Equal(t, float64(100), value.Get())
	})

	t.Run("SetValue - invalid value type", func(t *testing.T) {
		newValue, err := types.NullableAnyFrom("invalid")
		require.NoError(t, err)
		err = manager.SetValue(context.Background(), "resource1", newValue)
		assert.Error(t, err)
	})

	t.Run("SetValue - non-existent resource", func(t *testing.T) {
		newValue, err := types.NullableAnyFrom(100)
		require.NoError(t, err)
		err = manager.SetValue(context.Background(), "nonexistent", newValue)
		assert.Error(t, err)
	})
}

func TestResourceGroupManagerSave(t *testing.T) {
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

	t.Run("Save basic resource group", func(t *testing.T) {
		// Create a basic resource group
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

		rgm, err := NewResourceGroupManager(ctx, rgJson, nil)
		require.NoError(t, err)

		// Save the resource group
		err = rgm.Save(ctx)
		require.NoError(t, err)

		// Verify the resource group was saved
		rg, err := db.DB(ctx).GetResourceGroup(ctx, rgm.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		require.NoError(t, err)
		assert.NotNil(t, rg)
		assert.Equal(t, rgm.GetStoragePath(), rg.Path)
	})

	t.Run("Save resource group with different values", func(t *testing.T) {
		// Create first resource group
		rgJson1 := []byte(`{
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
							"name": "test1"
						}
					}
				}
			}
		}`)

		rgm1, err := NewResourceGroupManager(ctx, rgJson1, nil)
		require.NoError(t, err)

		// Save the first resource group
		err = rgm1.Save(ctx)
		require.NoError(t, err)

		// Get the first hash
		rg1, err := db.DB(ctx).GetResourceGroup(ctx, rgm1.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		require.NoError(t, err)
		hash1 := rg1.Hash

		// Create second resource group with different value
		rgJson2 := []byte(`{
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
							"name": "test2"
						}
					}
				}
			}
		}`)

		rgm2, err := NewResourceGroupManager(ctx, rgJson2, nil)
		require.NoError(t, err)

		// Save the second resource group
		err = rgm2.Save(ctx)
		require.NoError(t, err)

		// Get the second hash
		rg2, err := db.DB(ctx).GetResourceGroup(ctx, rgm2.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		require.NoError(t, err)
		hash2 := rg2.Hash

		// Verify hashes are different
		assert.NotEqual(t, hash1, hash2, "Hashes should be different for different values")
	})

	t.Run("Save resource group with different schema", func(t *testing.T) {
		// Create first resource group
		rgJson1 := []byte(`{
			"version": "v1",
			"kind": "ResourceGroup",
			"metadata": {
				"name": "test-rg3",
				"description": "Test resource group 3",
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

		rgm1, err := NewResourceGroupManager(ctx, rgJson1, nil)
		require.NoError(t, err)

		// Save the first resource group
		err = rgm1.Save(ctx)
		require.NoError(t, err)

		// Get the first hash
		rg1, err := db.DB(ctx).GetResourceGroup(ctx, rgm1.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		require.NoError(t, err)
		hash1 := rg1.Hash

		// Create second resource group with different schema
		rgJson2 := []byte(`{
			"version": "v1",
			"kind": "ResourceGroup",
			"metadata": {
				"name": "test-rg3",
				"description": "Test resource group 3",
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
								},
								"age": {
									"type": "number"
								}
							}
						},
						"value": {
							"name": "test",
							"age": 25
						}
					}
				}
			}
		}`)

		rgm2, err := NewResourceGroupManager(ctx, rgJson2, nil)
		require.NoError(t, err)

		// Save the second resource group
		err = rgm2.Save(ctx)
		require.NoError(t, err)

		// Get the second hash
		rg2, err := db.DB(ctx).GetResourceGroup(ctx, rgm2.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		require.NoError(t, err)
		hash2 := rg2.Hash

		// Verify hashes are different
		assert.NotEqual(t, hash1, hash2, "Hashes should be different for different schemas")
	})
}

func TestResourceGroupManagerDelete(t *testing.T) {
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

	t.Run("Delete existing resource group", func(t *testing.T) {
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

		// Verify the resource group exists
		rg, err := db.DB(ctx).GetResourceGroup(ctx, rgm.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		require.NoError(t, err)
		assert.NotNil(t, rg)

		// Delete the resource group
		err = rgm.Delete(ctx)
		require.NoError(t, err)

		// Verify the resource group is deleted
		rg, err = db.DB(ctx).GetResourceGroup(ctx, rgm.GetStoragePath(), variant.VariantID, variant.ResourceGroupsDirectoryID)
		assert.Error(t, err)
		assert.Nil(t, rg)
	})

	t.Run("Delete non-existent resource group", func(t *testing.T) {
		// Create a resource group but don't save it
		rgJson := []byte(`{
			"version": "v1",
			"kind": "ResourceGroup",
			"metadata": {
				"name": "non-existent-rg",
				"description": "Non-existent resource group",
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

		// Create the resource group manager
		rgm, err := NewResourceGroupManager(ctx, rgJson, nil)
		require.NoError(t, err)

		// Try to delete the non-existent resource group
		err = rgm.Delete(ctx)
		assert.Error(t, err)
	})

	t.Run("Delete resource group with invalid variant", func(t *testing.T) {
		// Create a resource group
		rgJson := []byte(`{
			"version": "v1",
			"kind": "ResourceGroup",
			"metadata": {
				"name": "test-rg2",
				"description": "Test resource group 2",
				"catalog": "test-catalog",
				"variant": "invalid-variant",
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

		// Create the resource group manager
		rgm, err := NewResourceGroupManager(ctx, rgJson, nil)
		require.NoError(t, err)

		// Try to delete with invalid variant
		err = rgm.Delete(ctx)
		assert.Error(t, err)
	})
}
