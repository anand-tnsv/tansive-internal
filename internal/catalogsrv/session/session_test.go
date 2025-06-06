package session

import (
	"errors"
	"testing"

	"github.com/jackc/pgtype"
	json "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/schema/errors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

func TestNewSession(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	// Create tenant and project
	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

	require.NoError(t, db.DB(ctx).CreateTenant(ctx, tenantID))
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	require.NoError(t, db.DB(ctx).CreateProject(ctx, projectID))
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	// Create a catalog
	catalogID := uuid.New()
	err := db.DB(ctx).CreateCatalog(ctx, &models.Catalog{
		CatalogID:   catalogID,
		Name:        "test-catalog",
		Description: "Test catalog",
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)
	defer db.DB(ctx).DeleteCatalog(ctx, uuid.Nil, "test-catalog")

	// Create a variant
	variantID := uuid.New()
	err = db.DB(ctx).CreateVariant(ctx, &models.Variant{
		VariantID:   variantID,
		Name:        "test-variant",
		Description: "Test variant",
		CatalogID:   catalogID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)

	// Set up catalog context
	ctx = catcommon.WithCatalogContext(ctx, &catcommon.CatalogContext{
		CatalogID: catalogID,
		Catalog:   "test-catalog",
		VariantID: variantID,
		Variant:   "test-variant",
		UserContext: &catcommon.UserContext{
			UserID: "users/testuser",
		},
	})

	// Create a skillset
	skillsetJson := []byte(`{
		"version": "v1",
		"kind": "SkillSet",
		"metadata": {
			"name": "test-skillset",
			"description": "Test skillset",
			"catalog": "test-catalog",
			"variant": "test-variant",
			"path": "/skills"
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
					"schema": {
						"type": "object",
						"properties": {
							"name": {
								"type": "string"
							}
						}
					}
				}
			],
			"skills": [
				{
					"name": "test-skill",
					"description": "Test skill",
					"inputSchema": {
						"type": "object",
						"properties": {
							"input": {
								"type": "string"
							}
						}
					},
					"outputSchema": {
						"type": "object",
						"properties": {
							"output": {
								"type": "string"
							}
						}
					},
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
	}`)

	// Create and save the skillset
	sm, err := catalogmanager.NewSkillSetManager(ctx, skillsetJson, nil)
	require.NoError(t, err)
	err = sm.Save(ctx)
	require.NoError(t, err)

	// Create a parent view
	parentView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "parent-view",
			"catalog": "test-catalog",
			"variant": "test-variant",
			"description": "Parent view for testing"
		},
		"spec": {
			"rules": [{
				"intent": "Allow",
				"actions": ["system.catalog.list", "system.variant.list", "system.namespace.list", "test.action"],
				"targets": ["res://*"]
			},
			{
				"intent": "Allow",
				"actions": ["system.catalog.adoptView"],
				"targets": ["res://views/parent-view"]
			}]
		}
	}`

	metadata := &interfaces.Metadata{
		Catalog: "test-catalog",
	}
	view, err := policy.CreateView(ctx, []byte(parentView), metadata)
	require.NoError(t, err)

	// Set the parent view in context
	var viewDef policy.ViewDefinition
	if err := json.Unmarshal(view.Rules, &viewDef); err != nil {
		t.Fatalf("failed to unmarshal view definition: %v", err)
	}
	ctx = policy.WithViewDefinition(ctx, &viewDef)

	tests := []struct {
		name        string
		sessionSpec string
		wantErr     bool
		errType     error
	}{
		{
			name: "valid session spec",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "parent-view",
				"variables": {
					"key1": "value1",
					"key2": 123,
					"key3": true
				}
			}`,
			wantErr: false,
		},
		{
			name: "missing skillPath",
			sessionSpec: `{
				"viewName": "parent-view",
				"variables": {
					"key1": "value1"
				}
			}`,
			wantErr: true,
			errType: ErrInvalidSession,
		},
		{
			name: "missing viewName",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"variables": {
					"key1": "value1"
				}
			}`,
			wantErr: true,
			errType: ErrInvalidSession,
		},
		{
			name: "invalid skillPath format",
			sessionSpec: `{
				"skillPath": "invalid/path/format",
				"viewName": "parent-view",
				"variables": {
					"key1": "value1"
				}
			}`,
			wantErr: true,
			errType: ErrInvalidSession,
		},
		{
			name: "invalid viewName format",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "invalid view name",
				"variables": {
					"key1": "value1"
				}
			}`,
			wantErr: true,
			errType: ErrInvalidSession,
		},
		{
			name: "too many variables",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "parent-view",
				"variables": {
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
					"key4": "value4",
					"key5": "value5",
					"key6": "value6",
					"key7": "value7",
					"key8": "value8",
					"key9": "value9",
					"key10": "value10",
					"key11": "value11",
					"key12": "value12",
					"key13": "value13",
					"key14": "value14",
					"key15": "value15",
					"key16": "value16",
					"key17": "value17",
					"key18": "value18",
					"key19": "value19",
					"key20": "value20",
					"key21": "value21"
				}
			}`,
			wantErr: true,
			errType: ErrInvalidSession,
		},
		{
			name: "invalid variable key format",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "parent-view",
				"variables": {
					"invalid@key": "value1"
				}
			}`,
			wantErr: true,
			errType: ErrInvalidSession,
		},
		{
			name: "invalid variable value type",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "parent-view",
				"variables": {
					"validKey": {"invalid": "object"}
				}
			}`,
			wantErr: false,
			errType: ErrInvalidSession,
		},
		{
			name: "non-existent view",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "non-existent-view",
				"variables": {
					"key1": "value1"
				}
			}`,
			wantErr: true,
			errType: ErrDisallowedByPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSession(ctx, []byte(tt.sessionSpec))
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					if tt.errType == ErrInvalidSession {
						// For validation errors, check if it's a schema validation error
						var validationErrors schemaerr.ValidationErrors
						if errors.As(err, &validationErrors) {
							assert.NotEmpty(t, validationErrors)
						} else {
							// If it's not a validation error, it should be ErrInvalidSession
							assert.True(t, errors.Is(err, tt.errType), "expected error to be %v, got %v", tt.errType, err)
						}
					} else {
						assert.True(t, errors.Is(err, tt.errType), "expected error to be %v, got %v", tt.errType, err)
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSessionSpec_Validate(t *testing.T) {
	tests := []struct {
		name    string
		spec    SessionSpec
		wantErr bool
	}{
		{
			name: "valid spec",
			spec: SessionSpec{
				SkillPath: "/skills/test-skill",
				ViewName:  "test-view",
				Variables: json.RawMessage(`{"key1": "value1"}`),
			},
			wantErr: false,
		},
		{
			name: "missing skillPath",
			spec: SessionSpec{
				ViewName:  "test-view",
				Variables: json.RawMessage(`{"key1": "value1"}`),
			},
			wantErr: true,
		},
		{
			name: "missing viewName",
			spec: SessionSpec{
				SkillPath: "skills/test-skill",
				Variables: json.RawMessage(`{"key1": "value1"}`),
			},
			wantErr: true,
		},
		{
			name: "invalid skillPath format",
			spec: SessionSpec{
				SkillPath: "invalid/path/format",
				ViewName:  "test-view",
				Variables: json.RawMessage(`{"key1": "value1"}`),
			},
			wantErr: true,
		},
		{
			name: "invalid viewName format",
			spec: SessionSpec{
				SkillPath: "skills/test-skill",
				ViewName:  "invalid view name",
				Variables: json.RawMessage(`{"key1": "value1"}`),
			},
			wantErr: true,
		},
		{
			name: "invalid variables format",
			spec: SessionSpec{
				SkillPath: "skills/test-skill",
				ViewName:  "test-view",
				Variables: json.RawMessage(`invalid json`),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Empty(t, err)
			}
		})
	}
}

func TestSessionSaveAndGet(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	// Create tenant and project
	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("P12345")
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

	require.NoError(t, db.DB(ctx).CreateTenant(ctx, tenantID))
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	require.NoError(t, db.DB(ctx).CreateProject(ctx, projectID))
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	// Create a catalog
	catalogID := uuid.New()
	err := db.DB(ctx).CreateCatalog(ctx, &models.Catalog{
		CatalogID:   catalogID,
		Name:        "test-catalog",
		Description: "Test catalog",
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)
	defer db.DB(ctx).DeleteCatalog(ctx, uuid.Nil, "test-catalog")

	// Create a variant
	variantID := uuid.New()
	err = db.DB(ctx).CreateVariant(ctx, &models.Variant{
		VariantID:   variantID,
		Name:        "test-variant",
		Description: "Test variant",
		CatalogID:   catalogID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	})
	require.NoError(t, err)

	// Set up catalog context
	ctx = catcommon.WithCatalogContext(ctx, &catcommon.CatalogContext{
		CatalogID: catalogID,
		Catalog:   "test-catalog",
		VariantID: variantID,
		Variant:   "test-variant",
		UserContext: &catcommon.UserContext{
			UserID: "users/testuser",
		},
	})

	// Create a skillset
	skillsetJson := []byte(`{
		"version": "v1",
		"kind": "SkillSet",
		"metadata": {
			"name": "test-skillset",
			"description": "Test skillset",
			"catalog": "test-catalog",
			"variant": "test-variant",
			"path": "/skills"
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
					"schema": {
						"type": "object",
						"properties": {
							"name": {
								"type": "string"
							}
						}
					}
				}
			],
			"skills": [
				{
					"name": "test-skill",
					"description": "Test skill",
					"inputSchema": {
						"type": "object",
						"properties": {
							"input": {
								"type": "string"
							}
						}
					},
					"outputSchema": {
						"type": "object",
						"properties": {
							"output": {
								"type": "string"
							}
						}
					},
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
	}`)

	// Create and save the skillset
	sm, err := catalogmanager.NewSkillSetManager(ctx, skillsetJson, nil)
	require.NoError(t, err)
	err = sm.Save(ctx)
	require.NoError(t, err)

	// Create a parent view
	parentView := `{
		"version": "v1",
		"kind": "View",
		"metadata": {
			"name": "parent-view",
			"catalog": "test-catalog",
			"variant": "test-variant",
			"description": "Parent view for testing"
		},
		"spec": {
			"rules": [{
				"intent": "Allow",
				"actions": ["system.catalog.list", "system.variant.list", "system.namespace.list", "test.action"],
				"targets": ["res://*"]
			},
			{
				"intent": "Allow",
				"actions": ["system.catalog.adoptView"],
				"targets": ["res://views/parent-view"]
			}]
		}
	}`

	metadata := &interfaces.Metadata{
		Catalog: "test-catalog",
	}
	view, err := policy.CreateView(ctx, []byte(parentView), metadata)
	require.NoError(t, err)

	// Set the parent view in context
	var viewDef policy.ViewDefinition
	if err := json.Unmarshal(view.Rules, &viewDef); err != nil {
		t.Fatalf("failed to unmarshal view definition: %v", err)
	}
	ctx = policy.WithViewDefinition(ctx, &viewDef)

	// Test cases
	tests := []struct {
		name        string
		sessionSpec string
		wantErr     bool
	}{
		{
			name: "valid session save and get",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "parent-view",
				"variables": {
					"key1": "value1",
					"key2": 123,
					"key3": true
				}
			}`,
			wantErr: false,
		},
		{
			name: "session with empty variables",
			sessionSpec: `{
				"skillPath": "/skills/test-skillset/test-skill",
				"viewName": "parent-view",
				"variables": {}
			}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and save session
			session, err := NewSession(ctx, []byte(tt.sessionSpec))
			require.NoError(t, err)

			err = session.Save(ctx)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Get session by ID
			gotSession, err := GetSession(ctx, session.(*sessionManager).session.SessionID)
			assert.NoError(t, err)
			assert.NotNil(t, gotSession)

			// Verify session details
			originalSession := session.(*sessionManager).session
			retrievedSession := gotSession.(*sessionManager).session

			assert.Equal(t, originalSession.SessionID, retrievedSession.SessionID)
			assert.Equal(t, originalSession.SkillSet, retrievedSession.SkillSet)
			assert.Equal(t, originalSession.Skill, retrievedSession.Skill)
			assert.Equal(t, originalSession.ViewID, retrievedSession.ViewID)

			// Compare parsed JSON values instead of raw bytes
			var originalVars, retrievedVars map[string]interface{}
			if err := json.Unmarshal(originalSession.Variables, &originalVars); err != nil {
				t.Fatalf("failed to unmarshal original variables: %v", err)
			}
			if err := json.Unmarshal(retrievedSession.Variables, &retrievedVars); err != nil {
				t.Fatalf("failed to unmarshal retrieved variables: %v", err)
			}
			assert.Equal(t, originalVars, retrievedVars)

			assert.Equal(t, originalSession.StatusSummary, retrievedSession.StatusSummary)
			assert.Equal(t, originalSession.UserID, retrievedSession.UserID)
			assert.Equal(t, originalSession.CatalogID, retrievedSession.CatalogID)
			assert.Equal(t, originalSession.VariantID, retrievedSession.VariantID)
		})
	}

	// Test error cases
	t.Run("get non-existent session", func(t *testing.T) {
		nonExistentID := uuid.New()
		_, err := GetSession(ctx, nonExistentID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidObject))
	})
}
