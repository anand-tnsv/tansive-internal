package catalogmanager

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/datatypes" // Import to register data types
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

func TestNewResourceGroupManager(t *testing.T) {
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
