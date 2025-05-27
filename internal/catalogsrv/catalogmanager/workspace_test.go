package catalogmanager

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
)

func TestNewWorkspaceManager(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected string
	}{
		{
			name: "valid workspace",
			jsonData: `
{
    "version": "v1",
    "kind": "Workspace",
    "metadata": {
		"name": "valid-workspace",
        "catalog": "valid-catalog",
        "variant": "valid-variant",
        "description": "This is a valid workspace"
    }
}`,

			expected: "",
		},
		{
			name: "invalid version",
			jsonData: `
		   {
		       "version": "v2",
		       "kind": "Workspace",
		       "metadata": {
				   "name": "valid-workspace2",
		           "catalog": "valid-catalog",
		           "variant": "valid-variant",
		           "description": "Invalid version in workspace"
		       }
		   }`,
			expected: ErrInvalidSchema.Error(),
		},

		{
			name: "invalid kind",
			jsonData: `
			   {
			       "version": "v1",
			       "kind": "InvalidKind",
			       "metadata": {
					   "name": "valid-workspace3",
			           "catalog": "valid-catalog",
			           "variant": "valid-variant",
			           "description": "Invalid kind in workspace"
			       }
			   }`,
			expected: ErrInvalidSchema.Error(),
		},
		{
			name:     "empty JSON data",
			jsonData: "",
			expected: ErrInvalidSchema.Error(),
		},
	}

	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("PDEFGH")

	// Set the tenant ID and project ID in the context
	ctx = catcommon.SetTenantIdInContext(ctx, tenantID)
	ctx = catcommon.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)

	// Create a catalog and variant for testing the workspace
	catalogName := "valid-catalog"
	c := models.Catalog{
		Name:        catalogName,
		Description: "Test catalog",
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}
	err = db.DB(ctx).CreateCatalog(ctx, &c)
	assert.NoError(t, err)

	variantName := "valid-variant"
	variant := models.Variant{
		Name:        variantName,
		Description: "Test variant",
		CatalogID:   c.CatalogID, // Set to catalog ID later
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}
	err = db.DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Convert JSON to []byte
			jsonData := []byte(tt.jsonData)

			// Create a new workspace manager
			wm, err := NewWorkspaceManager(ctx, jsonData, catalogName, variantName)
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}

			// Check if the error string matches the expected error string
			if errStr != tt.expected {
				t.Errorf("got error %v, expected error %v", err, tt.expected)
			} else if tt.expected == "" {
				// If no error is expected, validate workspace properties
				assert.NotNil(t, wm)
				assert.Equal(t, "This is a valid workspace", wm.Description())

				// Save the workspace
				err = wm.Save(ctx)
				assert.NotEqual(t, uuid.Nil, wm.ID())
				assert.NoError(t, err)

				// Attempt to save again to check for duplicate handling
				err = wm.Save(ctx)
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrAlreadyExists)
				assert.NotEqual(t, uuid.Nil, wm.ID())

				// Load the workspace
				loadedWorkspace, loadErr := LoadWorkspaceManagerByID(ctx, wm.ID())
				assert.NoError(t, loadErr)
				assert.NotEqual(t, uuid.Nil, loadedWorkspace.ID())
				assert.Equal(t, wm.Description(), loadedWorkspace.Description())

				// Load the workspace with an invalid ID
				_, loadErr = LoadWorkspaceManagerByID(ctx, uuid.New())
				assert.Error(t, loadErr)
				assert.ErrorIs(t, loadErr, ErrWorkspaceNotFound)

				// Delete the workspace
				err = DeleteWorkspace(ctx, wm.ID())
				assert.NoError(t, err)

				// Try loading the deleted workspace
				_, loadErr = LoadWorkspaceManagerByID(ctx, wm.ID())
				assert.Error(t, loadErr)
				assert.ErrorIs(t, loadErr, ErrWorkspaceNotFound)

				// Try deleting again to ensure no error is raised
				err = DeleteWorkspace(ctx, wm.ID())
				assert.NoError(t, err)
			}
		})
	}
}
