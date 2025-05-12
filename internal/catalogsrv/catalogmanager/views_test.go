package catalogmanager

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list"],
            "Target": ["res://catalog/validcatalog", "res://catalog/validcatalog/variant/my-variant"]
        }]
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
        "rules": []
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list"],
            "Target": ["res://catalog/validcatalog"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list"],
            "Target": ["res://catalog/validcatalog"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list"],
            "Target": ["res://catalog/validcatalog"]
        }]
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
        "rules": [{
            "Intent": "Invalid",
            "Operation": ["catalog.list"],
            "Target": ["res://catalog/validcatalog"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["Invalid"],
            "Target": ["res://catalog/validcatalog"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list"],
            "Target": ["invalid-uri", "res://invalid-format", "res://catalog/InvalidCase"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list", "variant.list", "namespace.list"],
            "Target": ["res://catalog/validcatalog", "res://catalog/validcatalog/variant/my-variant"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list", "InvalidAction", "variant.list"],
            "Target": ["res://catalog/validcatalog"]
        }]
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
        "rules": [{
            "Intent": "Allow",
            "Operation": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list", "namespace.list"],
            "Target": ["res://catalog/validcatalog", "res://catalog/validcatalog", "res://catalog/validcatalog/variant/my-variant", "res://catalog/validcatalog/variant/my-variant"]
        }]
    }
}`,
			expected: nil,
		},
	}

	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId(common.GetUniqueId(common.ID_TYPE_TENANT))
	projectID := types.ProjectId(common.GetUniqueId(common.ID_TYPE_PROJECT))

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

	// Create a catalog for testing the variants
	catalogName := "validcatalog"
	err = db.DB(ctx).CreateCatalog(ctx, &models.Catalog{
		Name:        catalogName,
		Description: "Test catalog",
		ProjectID:   projectID,
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

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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
			"rules": [
				{
					"Intent": "Allow",
					"Operation": ["catalog.list"],
					"Target": ["res://catalog/test-catalog"]
				}
			]
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
			"rules": [
				{
					"Intent": "Allow",
					"Operation": ["catalog.list", "variant.list", "namespace.list"],
					"Target": ["res://catalog/test-catalog"]
				}
			]
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
			"rules": [{
					"Intent": "Allow",
					"Operation": ["variant.list"],
					"Target": ["res://catalog/test-catalog"]
				}]
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
			"rules": []
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
			"rules": [
				{
					"Intent": "InvalidEffect",
					"Operation": ["catalog.list"],
					"Target": ["test/resource"]
				}
			]
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
			"rules": [
				{
					"Intent": "Allow",
					"Operation": ["catalog.list", "variant.list", "catalog.list", "namespace.list", "variant.list"],
					"Target": ["res://catalog/test-catalog", "res://catalog/test-catalog", "res://catalog/test-catalog/variant/valid-variant"]
				}
			]
		}
	}`

	_, err = UpdateView(ctx, []byte(updateViewWithDuplicates), "test-view", "test-catalog")
	require.NoError(t, err)

	// Verify the deduplication
	retrieved, err = db.DB(ctx).GetViewByLabel(ctx, "test-view", catalogID)
	require.NoError(t, err)

	var rules AccessRuleSet
	jsonErr := json.Unmarshal(retrieved.Rules, &rules)
	require.NoError(t, jsonErr)

	// Check that duplicates were removed
	assert.Equal(t, 1, len(rules))
	assert.Equal(t, 3, len(rules[0].Operation)) // Should have catalog.list, variant.list, namespace.list
	assert.Equal(t, 2, len(rules[0].Target))    // Should have two unique resources

	// Verify the order and content of deduplicated arrays
	expectedOperations := []Operation{OpCatalogList, OpVariantList, OpNamespaceList}
	assert.ElementsMatch(t, expectedOperations, rules[0].Operation)

	expectedTargets := []TargetResource{"res://catalog/test-catalog", "res://catalog/test-catalog/variant/valid-variant"}
	assert.ElementsMatch(t, expectedTargets, rules[0].Target)
}

func TestIsActionAllowed(t *testing.T) {
	tests := []struct {
		name           string
		rules          AccessRuleSet
		action         Operation
		resource       string
		expectedResult bool
	}{

		{
			name: "admin action",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogAdmin},
					Target:    []TargetResource{"res://catalog/*"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test2",
			expectedResult: true,
		},
		{
			name: "admin action with specific resource",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogAdmin},
					Target:    []TargetResource{"res://catalog/test1"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test2",
			expectedResult: false,
		},

		{
			name: "incorrectadmin action with specific resource",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogAdmin},
					Target:    []TargetResource{"res://catalog/test1/variant/test2"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test1",
			expectedResult: false,
		},
		{
			name: "incorrectadmin action with specific resource",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2",
						"res://catalog/*",
					},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test1",
			expectedResult: true,
		},
		{
			name: "allow namespace with admin action",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpNamespaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/namespace/*",
					},
				},
			},
			action:         OpNamespaceList,
			resource:       "res://catalog/test1/variant/test2/namespace/test3",
			expectedResult: true,
		},
		{
			name: "allow namespace with admin action and deny rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpNamespaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/namespace/*",
					},
				},
				{
					Intent:    ViewRuleEffectDeny,
					Operation: []Operation{OpNamespaceList},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/namespace/test3",
					},
				},
			},
			action:         OpNamespaceList,
			resource:       "res://catalog/test1/variant/test2/namespace/test3",
			expectedResult: false,
		},
		{
			name: "allow workspace with admin action",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpWorkspaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/namespace/test3/workspace/*",
					},
				},
			},
			action:         OpNamespaceList,
			resource:       "res://catalog/test1/variant/test2/namespace/test3/workspace/test4",
			expectedResult: false,
		},
		{
			name: "allow workspace with admin action",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpWorkspaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/workspace/*",
					},
				},
			},
			action:         OpNamespaceList,
			resource:       "res://catalog/test1/variant/test2/workspace/test4",
			expectedResult: true,
		},
		{
			name: "allow workspace with admin action",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpWorkspaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/workspace/*",
					},
				},
			},
			action:         OpNamespaceList,
			resource:       "res://catalog/test1/variant/test2/namespace/test3/workspace/test4",
			expectedResult: false,
		},
		{
			name: "allow workspace with admin action",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpWorkspaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/workspace/*",
					},
				},
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpNamespaceAdmin},
					Target: []TargetResource{
						"res://catalog/test1/variant/test2/namespace/*",
					},
				},
			},
			action:         OpNamespaceList,
			resource:       "res://catalog/test1/variant/test2/workspace/test4/namespace/test5",
			expectedResult: true,
		},

		{
			name: "simple allow rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test",
			expectedResult: true,
		},

		{
			name: "simple deny rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectDeny,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test",
			expectedResult: false,
		},

		{
			name: "deny overrides allow",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test"},
				},
				{
					Intent:    ViewRuleEffectDeny,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test",
			expectedResult: false,
		},

		{
			name: "wildcard resource matching",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test/variant/*"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test/variant/variant1",
			expectedResult: true,
		},

		{
			name: "multiple actions in rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList, OpVariantList},
					Target:    []TargetResource{"res://catalog/test"},
				},
			},
			action:         OpVariantList,
			resource:       "res://catalog/test",
			expectedResult: true,
		},
		{
			name: "action not in rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test"},
				},
			},
			action:         OpVariantList,
			resource:       "res://catalog/test",
			expectedResult: false,
		},

		{
			name: "resource not in rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/other",
			expectedResult: false,
		},

		{
			name: "multiple rules with different resources",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test1"},
				},
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test2"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test2",
			expectedResult: true,
		},

		{
			name: "wildcard resource with deny rule",
			rules: AccessRuleSet{
				{
					Intent:    ViewRuleEffectAllow,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test/*"},
				},
				{
					Intent:    ViewRuleEffectDeny,
					Operation: []Operation{OpCatalogList},
					Target:    []TargetResource{"res://catalog/test/specific"},
				},
			},
			action:         OpCatalogList,
			resource:       "res://catalog/test/specific",
			expectedResult: false,
		},
		{
			name:           "empty ruleset",
			rules:          AccessRuleSet{},
			action:         OpCatalogList,
			resource:       "res://catalog/test",
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
