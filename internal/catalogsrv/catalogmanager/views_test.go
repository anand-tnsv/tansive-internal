package catalogmanager

import (
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
            "Effect": "Allow",
            "Action": ["Read"],
            "Resource": ["res://catalog/validcatalog", "res://catalog/validcatalog/variant/my-variant"]
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
            "Effect": "Allow",
            "Action": ["Read"],
            "Resource": ["res://catalog/validcatalog"]
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
            "Effect": "Allow",
            "Action": ["Read"],
            "Resource": ["res://catalog/validcatalog"]
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
            "Effect": "Allow",
            "Action": ["Read"],
            "Resource": ["res://catalog/validcatalog"]
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
            "Effect": "Invalid",
            "Action": ["Read"],
            "Resource": ["res://catalog/validcatalog"]
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
            "Effect": "Allow",
            "Action": ["Invalid"],
            "Resource": ["res://catalog/validcatalog"]
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
            "Effect": "Allow",
            "Action": ["Read"],
            "Resource": ["invalid-uri", "res://invalid-format", "res://catalog/InvalidCase"]
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
            "Effect": "Allow",
            "Action": ["Read", "Write", "Execute"],
            "Resource": ["res://catalog/validcatalog", "res://catalog/validcatalog/variant/my-variant"]
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
            "Effect": "Allow",
            "Action": ["Read", "InvalidAction", "Write"],
            "Resource": ["res://catalog/validcatalog"]
        }]
    }
}`,
			expected: ErrInvalidSchema,
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
					"Effect": "Allow",
					"Action": ["Read"],
					"Resource": ["res://catalog/test-catalog"]
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
					"Effect": "Allow",
					"Action": ["Read", "Write", "Execute"],
					"Resource": ["res://catalog/test-catalog"]
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
					"Effect": "Allow",
					"Action": ["Write"],
					"Resource": ["res://catalog/test-catalog"]
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
					"Effect": "InvalidEffect",
					"Action": ["Read"],
					"Resource": ["test/resource"]
				}
			]
		}
	}`

	_, err = UpdateView(ctx, []byte(invalidSchemaView), "", "test-catalog")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidSchema))
}
