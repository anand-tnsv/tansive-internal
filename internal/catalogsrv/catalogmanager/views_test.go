package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestCreateView(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected apperrors.Error
	}{
		{
			name: "valid view",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "valid-view",
		        "catalog": "validcatalog",
		        "description": "This is a valid view"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "validcatalog"
		            },
		            "rules": [{
		                "intent": "Allow",
		                "actions": ["catalog.list"],
		                "targets": ["res://catalogs/validcatalog", "res://catalogs/validcatalog/variants/my-variant/resources/coll-schema"]
		            }]
		        }
		    }
		}`,
			expected: nil,
		},
		{
			name: "valid view",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "valid-view2",
		        "catalog": "validcatalog",
		        "description": "This is a valid view"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "validcatalog"
		            },
		            "rules": [{
		                "intent": "Allow",
		                "actions": ["catalog.list"],
		                "targets": ["res://catalogs/validcatalog", "res://catalogs/validcatalog/variants/my-variant/collectionschemas"]
		            }]
		        }
		    }
		}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "valid view",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "valid-view3",
		        "catalog": "validcatalog",
		        "description": "This is a valid view"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "validcatalog"
		            },
		            "rules": [{
		                "intent": "Allow",
		                "actions": ["catalog.list"],
		                "targets": ["res://catalogs/validcatalog", "res://resources/*"]
		            }]
		        }
		    }
		}`,
			expected: nil,
		},
		{
			name: "valid view",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "valid-view4",
		        "catalog": "validcatalog",
		        "description": "This is a valid view"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "validcatalog"
		            },
		            "rules": [{
		                "intent": "Allow",
		                "actions": ["catalog.list"],
		                "targets": ["res://catalogs/validcatalog", "res://resources/*"]
		            }]
		        }
		    }
		}`,
			expected: nil,
		},
		{
			name: "empty rules",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "empty-rules-view",
				        "catalog": "validcatalog",
				        "description": "View with empty rules"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": []
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid version",
			jsonData: `
				{
				    "version": "v2",
				    "kind": "View",
				    "metadata": {
				        "name": "invalid-version-view",
				        "catalog": "validcatalog",
				        "description": "Invalid version in view"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list"],
				                "targets": ["res://catalogs/validcatalog"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid kind",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "InvalidKind",
				    "metadata": {
				        "name": "invalid-kind-view",
				        "catalog": "validcatalog",
				        "description": "Invalid kind in view"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list"],
				                "targets": ["res://catalogs/validcatalog"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid name format",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "invalid name format",
				        "catalog": "validcatalog",
				        "description": "Invalid name format in view"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list"],
				                "targets": ["res://catalogs/validcatalog"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid rule effect",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "invalid-rule-effect",
				        "catalog": "validcatalog",
				        "description": "Invalid rule effect in view"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Invalid",
				                "actions": ["catalog.list"],
				                "targets": ["res://catalogs/validcatalog"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid rule action",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "invalid-rule-action",
				        "catalog": "validcatalog",
				        "description": "Invalid rule action in view"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["Invalid"],
				                "targets": ["res://catalogs/validcatalog"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid resource URI",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "invalid-resource-uri",
				        "catalog": "validcatalog",
				        "description": "Invalid resource URI in view"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list"],
				                "targets": ["invalid-uri", "res://invalid-format", "res://catalogs/InvalidCase"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "valid view with multiple actions",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "valid-view-multi-action",
				        "catalog": "validcatalog",
				        "description": "This is a valid view with multiple actions"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list", "variant.list", "namespace.list"],
				                "targets": ["res://catalogs/validcatalog", "res://catalogs/validcatalog/variants/my-variant"]
				            }]
				        }
				    }
				}`,
			expected: nil,
		},
		{
			name: "invalid rule action with mixed valid and invalid",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "invalid-mixed-actions",
				        "catalog": "validcatalog",
				        "description": "View with mixed valid and invalid actions"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list", "InvalidAction", "variant.list"],
				                "targets": ["res://catalogs/validcatalog"]
				            }]
				        }
				    }
				}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "deduplication of actions and resources",
			jsonData: `
				{
				    "version": "v1",
				    "kind": "View",
				    "metadata": {
				        "name": "dedup-test-view",
				        "catalog": "validcatalog",
				        "description": "Test view for deduplication"
				    },
				    "spec": {
				        "definition": {
				            "scope": {
				                "catalog": "validcatalog"
				            },
				            "rules": [{
				                "intent": "Allow",
				                "actions": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list", "namespace.list"],
				                "targets": ["res://catalogs/validcatalog", "res://catalogs/validcatalog", "res://catalogs/validcatalog/variants/my-variant", "res://catalogs/validcatalog/variants/my-variant"]
				            }]
				        }
				    }
				}`,
			expected: nil,
		},
		{
			name: "test with scopes",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "scope-override-test",
		        "catalog": "validcatalog",
		        "description": "Test view for deduplication"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "validcatalog1",
						"variant": "validvariant1",
						"namespace": "validnamespace1"
		            },
		            "rules": [{
		                "intent": "Allow",
		                "actions": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list", "namespace.list"],
		                "targets": ["res://catalogs/validcatalog", "res://catalogs/validcatalog", "res://catalogs/validcatalog/variants/my-variant", "res://catalogs/validcatalog/variants/my-variant/resources/my-workspace"]
		            }]
		        }
		    }
		}`,
			expected: nil,
		},
		{
			name: "test with resource URI",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "test-with-resource-uri",
		        "catalog": "validcatalog",
		        "description": "Test view for deduplication"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "validcatalog1",
						"variant": "validvariant1",
						"namespace": "validnamespace1"
		            },
		            "rules": [{
		                "intent": "Allow",
		                "actions": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list", "namespace.list"],
		                "targets": ["res://resources/my-collection", "res://namespaces/*/resources/a/b/c/d"]
		            },
					{
		                "intent": "Allow",
		                "actions": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list", "namespace.list"],
		                "targets": []
		            }]
		        }
		    }
		}`,
			expected: nil,
		},
		{
			name: "test with adopt view",
			jsonData: `
		{
		    "version": "v1",
		    "kind": "View",
		    "metadata": {
		        "name": "test-with-adopt-view",
		        "catalog": "validcatalog",
		        "description": "Test view for deduplication"
		    },
		    "spec": {
		        "definition": {
		            "scope": {
		                "catalog": "my-catalog"
		            },
		            "rules": [
					{
		                "intent": "Allow",
		                "actions": ["catalog.adoptView"],
		                "targets": ["res://views/some-view/variants/test-variant"]
		            },
					{
		                "intent": "Allow",
		                "actions": ["catalog.admin"],
		                "targets": []
		            }
					]
		        }
		    }
		}`,
			expected: nil,
		},
	}

	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID, goerr := common.GetUniqueId(common.ID_TYPE_TENANT)
	require.NoError(t, goerr)
	projectID, goerr := common.GetUniqueId(common.ID_TYPE_PROJECT)
	require.NoError(t, goerr)

	// Set the tenant ID and project ID in the context
	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))
	ctx = catcommon.WithProjectID(ctx, catcommon.ProjectId(projectID))

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, catcommon.TenantId(tenantID))
	require.NoError(t, err)
	defer db.DB(ctx).DeleteTenant(ctx, catcommon.TenantId(tenantID))

	err = db.DB(ctx).CreateProject(ctx, catcommon.ProjectId(projectID))
	require.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, catcommon.ProjectId(projectID))

	// Create a catalog for testing the variants
	catalogName := "validcatalog"
	err = db.DB(ctx).CreateCatalog(ctx, &models.Catalog{
		Name:        catalogName,
		Description: "Test catalog",
		ProjectID:   catcommon.ProjectId(projectID),
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)
	defer db.DB(ctx).DeleteCatalog(ctx, uuid.Nil, catalogName)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CreateView(ctx, []byte(tt.jsonData), "")
			if tt.expected == nil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tt.expected), "expected error to be %v", tt.expected)
			}
		})
	}
}

func TestUpdateView(t *testing.T) {
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

	require.NoError(t, db.DB(ctx).CreateTenant(ctx, tenantID))
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	require.NoError(t, db.DB(ctx).CreateProject(ctx, projectID))
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	// Create a catalog first
	catalogID := uuid.New()
	err := db.DB(ctx).CreateCatalog(ctx, &models.Catalog{
		CatalogID:   catalogID,
		Name:        "test-catalog",
		Description: "Test catalog",
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)

	// Create initial view
	initialView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "test-view",
			"catalog": "test-catalog",
			"description": "Initial description"
		},
		"spec": {
			"definition": {
				"scope": {
					"catalog": "test-catalog"
				},
				"rules": [
					{
						"intent": "Allow",
						"actions": ["catalog.list"],
						"targets": ["res://catalogs/test-catalog"]
					}
				]
			}
		}
	}`

	_, err = CreateView(ctx, []byte(initialView), "")
	require.NoError(t, err)

	// Test successful update with multiple actions
	updateView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "test-view",
			"catalog": "test-catalog",
			"description": "Updated description"
		},
		"spec": {
			"definition": {
				"scope": {
					"catalog": "test-catalog"
				},
				"rules": [
					{
						"intent": "Allow",
						"actions": ["catalog.list", "variant.list", "namespace.list"],
						"targets": ["res://catalogs/test-catalog"]
					}
				]
			}
		}
	}`

	_, err = UpdateView(ctx, []byte(updateView), "test-view", "test-catalog")
	require.NoError(t, err)

	// Verify the update
	retrieved, err := db.DB(ctx).GetViewByLabel(ctx, "test-view", catalogID)
	require.NoError(t, err)
	assert.Equal(t, "Updated description", retrieved.Description)

	// Test updating non-existent view
	nonExistentView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "non-existent-view",
			"catalog": "test-catalog",
			"description": "Should fail"
		},
		"spec": {
			"definition": {
				"scope": {
					"catalog": "test-catalog"
				},
				"rules": [{
						"intent": "Allow",
						"actions": ["variant.list"],
						"targets": ["res://catalogs/test-catalog"]
					}]
			}
		}
	}`

	_, err = UpdateView(ctx, []byte(nonExistentView), "", "test-catalog")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrViewNotFound))

	// Test updating with invalid catalog
	invalidCatalogView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "test-view",
			"catalog": "non-existent-catalog",
			"description": "Should fail"
		},
		"spec": {
			"definition": {
				"scope": {
					"catalog": "non-existent-catalog"
				},
				"rules": []
			}
		}
	}`

	_, err = UpdateView(ctx, []byte(invalidCatalogView), "", "test-catalog")
	assert.Error(t, err)

	// Test updating with invalid JSON
	_, err = UpdateView(ctx, []byte("invalid json"), "", "test-catalog")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidView))

	// Test updating with invalid schema
	invalidSchemaView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "test-view",
			"catalog": "test-catalog"
		},
		"spec": {
			"definition": {
				"scope": {
					"catalog": "test-catalog"
				},
				"rules": [
					{
						"intent": "InvalidEffect",
						"actions": ["catalog.list"],
						"targets": ["res://catalogs/test-catalog"]
					}
				]
			}
		}
	}`

	_, err = UpdateView(ctx, []byte(invalidSchemaView), "", "test-catalog")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidSchema))

	// Test deduplication in update
	updateViewWithDuplicates := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "test-view",
			"catalog": "test-catalog",
			"description": "Updated description with duplicates"
		},
		"spec": {
			"definition": {
				"scope": {
					"catalog": "test-catalog"
				},
				"rules": [
					{
						"intent": "Allow",
						"actions": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list"],
						"targets": ["res://catalogs/test-catalog", "res://catalogs/test-catalog", "res://catalogs/test-catalog/variants/valid-variant"]
					}
				]
			}
		}
	}`

	_, err = UpdateView(ctx, []byte(updateViewWithDuplicates), "test-view", "test-catalog")
	require.NoError(t, err)

	// Verify the deduplication
	retrieved, err = db.DB(ctx).GetViewByLabel(ctx, "test-view", catalogID)
	require.NoError(t, err)

	var definition types.ViewDefinition
	jsonErr := json.Unmarshal(retrieved.Rules, &definition)
	require.NoError(t, jsonErr)

	// Check that duplicates were removed
	assert.Equal(t, 1, len(definition.Rules))
	assert.Equal(t, 3, len(definition.Rules[0].Actions)) // Should have catalog.list, variant.list, namespace.list
	assert.Equal(t, 2, len(definition.Rules[0].Targets)) // Should have two unique resources

	// Verify the order and content of deduplicated arrays
	expectedOperations := []types.Action{types.ActionCatalogList, types.ActionVariantList, types.ActionNamespaceList}
	assert.ElementsMatch(t, expectedOperations, definition.Rules[0].Actions)

	expectedTargets := []types.TargetResource{"res://catalogs/test-catalog", "res://catalogs/test-catalog/variants/valid-variant"}
	assert.ElementsMatch(t, expectedTargets, definition.Rules[0].Targets)
}

func TestIsActionAllowed(t *testing.T) {
	tests := []struct {
		name           string
		rules          types.Rules
		action         types.Action
		resource       types.TargetResource
		expectedResult bool
	}{
		{
			name: "admin action",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogAdmin},
					Targets: []types.TargetResource{"res://catalogs/*"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test2",
			expectedResult: true,
		},
		{
			name: "admin action with specific resource",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogAdmin},
					Targets: []types.TargetResource{"res://catalogs/test1"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test2",
			expectedResult: false,
		},
		{
			name: "incorrectadmin action with specific resource",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogAdmin},
					Targets: []types.TargetResource{"res://catalogs/test1/variants/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test1",
			expectedResult: false,
		},
		{
			name: "incorrectadmin action with specific resource",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogAdmin},
					Targets: []types.TargetResource{
						"res://catalogs/test1/variants/test2",
						"res://catalogs/*",
					},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test1",
			expectedResult: true,
		},
		{
			name: "allow namespace with admin action",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionNamespaceAdmin},
					Targets: []types.TargetResource{
						"res://catalogs/test1/variants/test2/namespaces/*",
					},
				},
			},
			action:         types.ActionNamespaceList,
			resource:       "res://catalogs/test1/variants/test2/namespaces/test3",
			expectedResult: true,
		},
		{
			name: "allow namespace with admin action and deny rule",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionNamespaceAdmin},
					Targets: []types.TargetResource{
						"res://catalogs/test1/variants/test2/namespaces/*",
					},
				},
				{
					Intent:  types.IntentDeny,
					Actions: []types.Action{types.ActionNamespaceList},
					Targets: []types.TargetResource{
						"res://catalogs/test1/variants/test2/namespaces/test3",
					},
				},
			},
			action:         types.ActionNamespaceList,
			resource:       "res://catalogs/test1/variants/test2/namespaces/test3",
			expectedResult: false,
		},
		{
			name: "simple allow rule",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test",
			expectedResult: true,
		},
		{
			name: "simple deny rule",
			rules: types.Rules{
				{
					Intent:  types.IntentDeny,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test",
			expectedResult: false,
		},
		{
			name: "deny overrides allow",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
				{
					Intent:  types.IntentDeny,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test",
			expectedResult: false,
		},
		{
			name: "wildcard resource matching",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test/variants/*"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test/variants/variant1",
			expectedResult: true,
		},
		{
			name: "multiple actions in rule",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
			},
			action:         types.ActionVariantList,
			resource:       "res://catalogs/test",
			expectedResult: true,
		},
		{
			name: "action not in rule",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
			},
			action:         types.ActionVariantList,
			resource:       "res://catalogs/test",
			expectedResult: false,
		},
		{
			name: "resource not in rule",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/other",
			expectedResult: false,
		},
		{
			name: "multiple rules with different resources",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test1"},
				},
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test2",
			expectedResult: true,
		},
		{
			name: "wildcard resource with deny rule",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test/*"},
				},
				{
					Intent:  types.IntentDeny,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/test/specific"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test/specific",
			expectedResult: false,
		},
		{
			name:           "empty ruleset",
			rules:          types.Rules{},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test",
			expectedResult: false,
		},
		{
			name: "mismatched action",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/*", "res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/test2",
			expectedResult: true,
		},
		{
			name: "varying length of resource",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/my-namespace/resources/some-schema",
						"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/*/namespaces/my-namespace",
			expectedResult: false,
		},
		{
			name: "varying length of resource",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/my-namespace/resources/some-schema",
						"res://catalogs/test2"},
				},
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionNamespaceAdmin},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/my-namespace"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace",
			expectedResult: true,
		},
		{
			name: "varying length of resource2",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/my-namespace",
						"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/resources/some-schema",
			expectedResult: false,
		},
		{
			name: "varying length of resource3",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionNamespaceList},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/*",
						"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/resources/some-schema",
			expectedResult: false,
		},
		{
			name: "varying length of resource3",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogList},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/*",
						"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/resources/some-schema",
			expectedResult: true,
		},
		{
			name: "varying length of resource3",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionNamespaceAdmin},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/*",
						"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/resources/some-schema",
			expectedResult: true,
		},
		{
			name: "varying length of resource3",
			rules: types.Rules{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionVariantAdmin},
					Targets: []types.TargetResource{"res://catalogs/my-catalog/variants/*/namespaces/*",
						"res://catalogs/test2"},
				},
			},
			action:         types.ActionCatalogList,
			resource:       "res://catalogs/my-catalog/variants/my-variant/namespaces/my-namespace/resources/some-schema",
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.rules.IsActionAllowed(tt.action, tt.resource)
			assert.Equal(t, tt.expectedResult, result, "IsActionAllowed(%v, %v) = %v, want %v", tt.action, tt.resource, result, tt.expectedResult)
		})
	}
}

func TestValidateDerivedView(t *testing.T) {
	tests := []struct {
		name        string
		parent      types.ViewDefinition
		child       types.ViewDefinition
		expectError bool
	}{
		{
			name: "valid derived view",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid derived view - different scope",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/test"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
					Variant: "test-variant",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "valid derivation - subset of actions",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList, types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid derivation - child has more actions",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "valid derivation - parent with wildcard",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/*"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid derivation - parent with wildcard",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogAdoptView, types.ActionVariantList},
						Targets: []types.TargetResource{"res://variant/test-variant"},
					},
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogAdoptView},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogAdoptView},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionVariantClone},
						Targets: []types.TargetResource{"res://variant/test-variant"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "invalid derivation - child with wildcard, parent specific",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/*"},
					},
				},
			},
			expectError: false, //TODO - Check this. It must be true
		},
		{
			name: "valid derivation - parent with admin permission",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogAdmin},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid derivation - with deny rules",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/*"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/*"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid derivation - child doesn't need parent's deny rule",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList, types.ActionVariantList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/*"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionCatalogList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid derivation - child allows denied resource",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/*"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/restricted"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/*"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "valid derivation - child respects parent's deny with specific allow",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/*"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/restricted"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/namespaces/allowed"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid derivation - parent denies specific action in wildcard",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceAdmin},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/variants/*/namespaces/*"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/variants/*/namespaces/restricted"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceList, types.ActionNamespaceCreate},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/variants/*/namespaces/allowed"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid derivation - child allows denied action in wildcard",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceAdmin},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/variants/*/namespaces/*"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/variants/*/namespaces/restricted"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionNamespaceList},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/variants/*/namespaces/*"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "valid derivation - parent denies subset of allowed actions",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionResourceRead, types.ActionResourceEdit, types.ActionResourceGet},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/resources/*"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionResourceEdit, types.ActionResourceGet},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/resources/sensitive/*"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionResourceRead},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/resources/*"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid derivation - child allows action denied for specific resource pattern",
			parent: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionResourceRead, types.ActionResourceEdit, types.ActionResourceGet},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/resources/*"},
					},
					{
						Intent:  types.IntentDeny,
						Actions: []types.Action{types.ActionResourceEdit, types.ActionResourceGet},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/resources/sensitive/*"},
					},
				},
			},
			child: types.ViewDefinition{
				Scope: types.Scope{
					Catalog: "test-catalog",
				},
				Rules: types.Rules{
					{
						Intent:  types.IntentAllow,
						Actions: []types.Action{types.ActionResourceRead, types.ActionResourceEdit},
						Targets: []types.TargetResource{"res://catalogs/test-catalog/resources/*"},
					},
				},
			},
			expectError: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDerivedView(ctx, &tt.parent, &tt.child)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeleteView(t *testing.T) {
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

	require.NoError(t, db.DB(ctx).CreateTenant(ctx, tenantID))
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	require.NoError(t, db.DB(ctx).CreateProject(ctx, projectID))
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	// Create a catalog first
	catalogID := uuid.New()
	err := db.DB(ctx).CreateCatalog(ctx, &models.Catalog{
		CatalogID:   catalogID,
		Name:        "test-catalog",
		Description: "Test catalog",
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)

	// Create test views
	testViews := []struct {
		name        string
		label       string
		description string
	}{
		{
			name:        "view1",
			label:       "test-view-1",
			description: "Test view 1",
		},
		{
			name:        "view2",
			label:       "test-view-2",
			description: "Test view 2",
		},
	}

	for _, tv := range testViews {
		view := `{
			"version": "v1",
			"kind": "View",
			"metadata": {
				"name": "%s",
				"catalog": "test-catalog",
				"description": "%s"
			},
			"spec": {
				"definition": {
					"scope": {
						"catalog": "test-catalog"
					},
					"rules": [
						{
							"intent": "Allow",
							"actions": ["catalog.list"],
							"targets": ["res://catalogs/test-catalog"]
						}
					]
				}
			}
		}`
		viewJSON := fmt.Sprintf(view, tv.label, tv.description)
		_, err = CreateView(ctx, []byte(viewJSON), "")
		require.NoError(t, err)
	}

	t.Run("delete by label - success", func(t *testing.T) {
		// Delete first view by label
		reqCtx := interfaces.RequestContext{
			CatalogID:  catalogID,
			Catalog:    "test-catalog",
			ObjectName: "test-view-1",
		}
		vr, err := NewViewKindHandler(ctx, reqCtx)
		require.NoError(t, err)

		err = vr.Delete(ctx)
		assert.NoError(t, err)

		// Verify view is deleted
		_, err = db.DB(ctx).GetViewByLabel(ctx, "test-view-1", catalogID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, dberror.ErrNotFound))
	})

	t.Run("delete by label - non-existent view", func(t *testing.T) {
		reqCtx := interfaces.RequestContext{
			CatalogID:  catalogID,
			Catalog:    "test-catalog",
			ObjectName: "non-existent-view",
		}
		vr, err := NewViewKindHandler(ctx, reqCtx)
		require.NoError(t, err)

		err = vr.Delete(ctx)
		assert.NoError(t, err) // Should return nil for non-existent view
	})

	t.Run("delete by label - invalid catalog ID", func(t *testing.T) {
		reqCtx := interfaces.RequestContext{
			CatalogID:  uuid.Nil,
			Catalog:    "test-catalog",
			ObjectName: "test-view-2",
		}
		_, err := NewViewKindHandler(ctx, reqCtx)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidCatalog))
	})

	t.Run("delete by label - empty label", func(t *testing.T) {
		reqCtx := interfaces.RequestContext{
			CatalogID:  catalogID,
			Catalog:    "test-catalog",
			ObjectName: "",
		}
		vr, err := NewViewKindHandler(ctx, reqCtx)
		require.NoError(t, err)

		err = vr.Delete(ctx)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidView))
	})

	t.Run("delete by label - wrong catalog ID", func(t *testing.T) {
		wrongCatalogID := uuid.New()
		reqCtx := interfaces.RequestContext{
			CatalogID:  wrongCatalogID,
			Catalog:    "test-catalog",
			ObjectName: "test-view-2",
		}
		vr, err := NewViewKindHandler(ctx, reqCtx)
		require.NoError(t, err)

		err = vr.Delete(ctx)
		assert.NoError(t, err) // Should return nil as the view doesn't exist in this catalog
	})
}

func TestMorphViewDefinition(t *testing.T) {
	vdjson := `
		{
			"scope": {
				"catalog": "validcatalog"
			},
			"rules": [{
				"intent": "Allow",
				"actions": ["catalog.list"],
				"targets": ["res://catalogs/*", "res://variants/my-variant/resources/coll-schema"]
			}]
		}`
	vd := &types.ViewDefinition{}
	err := json.Unmarshal([]byte(vdjson), &vd)
	require.NoError(t, err)

	vd = CanonicalizeViewDefinition(vd)
	assert.Equal(t, &types.ViewDefinition{
		Scope: types.Scope{
			Catalog: "validcatalog",
		},
		Rules: types.Rules{
			{
				Intent:  types.IntentAllow,
				Actions: []types.Action{types.ActionCatalogList},
				Targets: []types.TargetResource{"res://catalogs/validcatalog", "res://catalogs/validcatalog/variants/my-variant/resources/coll-schema"},
			},
		},
	}, vd)
}
