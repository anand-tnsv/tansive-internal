package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/session"
)

func TestSessionCrud(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("PABCDE")

	config.Config().DefaultProjectID = string(projectID)
	config.Config().DefaultTenantID = string(tenantID)

	// Set the tenant ID and project ID in the context
	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	testContext := TestContext{
		TenantId:       tenantID,
		ProjectId:      projectID,
		CatalogContext: catcommon.CatalogContext{},
	}

	// Create a catalog
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().Auth.FakeSingleUserToken)
	response := executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Catalog = "valid-catalog"

	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().Auth.FakeSingleUserToken)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Variant = "valid-variant"

	// Create a skillset
	httpReq, _ = http.NewRequest("POST", "/skillsets", nil)
	req = `
		{
			"version": "v1",
			"kind": "SkillSet",
			"metadata": {
				"name": "valid-skillset",
				"catalog": "valid-catalog",
				"variant": "valid-variant",
				"namespace": "",
				"path": "/",
				"description": "This is a valid skillset"
			},
			"spec": {
				"version": "1.0.0",
				"runners": [
					{
						"name": "command-runner",
						"id": "system.commandrunner",
						"config": {
							"command": "python3 test.py"
						}
					}
				],
				"skills": [
					{
						"name": "test-skill",
						"description": "Test skill",
						"source": "command-runner",
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
				]
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a view
	httpReq, _ = http.NewRequest("POST", "/views", nil)
	req = `
		{
			"version": "v1",
			"kind": "View",
			"metadata": {
				"name": "valid-view",
				"catalog": "valid-catalog",
				"variant": "valid-variant",
				"description": "This is a valid view"
			},
			"spec": {
				"rules": [
					{
						"intent": "Allow",
						"actions": ["test.action"],
						"targets": ["res://*"]
					},
					{
						"intent": "Allow",
						"actions": ["system.catalog.adoptView"],
						"targets": ["res://views/valid-view"]
					},
					{
						"intent": "Allow",
						"actions": ["system.skillset.use"],
						"targets": ["res://skillsets/valid-skillset"]
					}
				]
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a session
	httpReq, _ = http.NewRequest("POST", "/sessions", nil)
	req = `
		{
			"skillPath": "/valid-skillset/test-skill",
			"viewName": "valid-view",
			"sessionVariables": {
				"key1": "value1",
				"key2": 123,
				"key3": true
			},
			"inputArgs": {
				"input": "test input"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Extract session ID from Location header
	location := response.Header().Get("Location")
	require.Equal(t, http.StatusCreated, response.Code)
	require.NotEmpty(t, location)

	// Test execution state creation and retrieval
	t.Run("execution state flow", func(t *testing.T) {
		codeVerifier := "test_challenge"
		hashed := sha256.Sum256([]byte(codeVerifier))
		codeChallenge := base64.RawURLEncoding.EncodeToString(hashed[:])
		// First create an interactive session
		httpReq, _ := http.NewRequest("POST", "/sessions?interactive=true&code_challenge="+codeChallenge, nil)
		req := `
			{
				"skillPath": "/valid-skillset/test-skill",
				"viewName": "valid-view",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test input"
				}
			}`
		setRequestBodyAndHeader(t, httpReq, req)
		response := executeTestRequest(t, httpReq, nil, testContext)
		assert.Equal(t, http.StatusOK, response.Code)

		var sessionResp session.InteractiveSessionRsp
		err := json.Unmarshal(response.Body.Bytes(), &sessionResp)
		assert.NoError(t, err)
		assert.NotEmpty(t, sessionResp.Code)

		// Create execution state with the code
		newCtx := newDb()
		t.Cleanup(func() {
			db.DB(newCtx).Close(newCtx)
		})

		httpReq, _ = http.NewRequest("POST", "/sessions/execution-state?code="+sessionResp.Code+"&code_verifier="+codeVerifier, nil)
		response = executeTestRequest(t, httpReq, nil)
		require.Equal(t, http.StatusOK, response.Code)

		var tokenResp session.SessionTokenRsp
		err = json.Unmarshal(response.Body.Bytes(), &tokenResp)
		assert.NoError(t, err)
		assert.NotEmpty(t, tokenResp.Token)
		assert.False(t, tokenResp.Expiry.IsZero())

		// Get execution state using the token
		newCtx2 := newDb()
		t.Cleanup(func() {
			db.DB(newCtx2).Close(newCtx2)
		})

		// Extract session ID from the token response
		httpReq, _ = http.NewRequest("GET", "/sessions/execution-state", nil)
		httpReq.Header.Set("Authorization", "Bearer "+tokenResp.Token)
		response = executeTestRequest(t, httpReq, nil)
		require.Equal(t, http.StatusOK, response.Code)

		var executionState session.ExecutionState
		err = json.Unmarshal(response.Body.Bytes(), &executionState)
		assert.NoError(t, err)
		assert.Equal(t, "/valid-skillset", executionState.SkillSet)
		assert.Equal(t, "test-skill", executionState.Skill)
		assert.Equal(t, "valid-view", executionState.View)
		assert.Equal(t, "valid-catalog", executionState.Catalog)
		assert.Equal(t, "valid-variant", executionState.Variant)
		assert.Equal(t, tenantID, executionState.TenantID)
		assert.Equal(t, "value1", executionState.SessionVariables["key1"])
		assert.Equal(t, "test input", executionState.InputArgs["input"])
	})

	// // Test interactive session creation
	t.Run("interactive session creation", func(t *testing.T) {
		config.Config().DefaultTenantID = string(tenantID)
		config.Config().DefaultProjectID = string(projectID)

		httpReq, _ := http.NewRequest("POST", "/sessions?interactive=true&code_challenge=test_challenge", nil)
		req := `
			{
				"skillPath": "/valid-skillset/test-skill",
				"viewName": "valid-view",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test input"
				}
			}`
		setRequestBodyAndHeader(t, httpReq, req)
		response := executeTestRequest(t, httpReq, nil, testContext)
		assert.Equal(t, http.StatusOK, response.Code)

		var sessionResp session.InteractiveSessionRsp
		err := json.Unmarshal(response.Body.Bytes(), &sessionResp)
		assert.NoError(t, err)
		assert.NotEmpty(t, sessionResp.Code)
		assert.NotEmpty(t, sessionResp.TangentURL)
	})

	// // Test interactive session without code challenge
	t.Run("interactive session without code challenge", func(t *testing.T) {
		httpReq, _ := http.NewRequest("POST", "/sessions?interactive=true", nil)
		req := `
			{
				"skillPath": "/valid-skillset/test-skill",
				"viewName": "valid-view",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test input"
				}
			}`
		setRequestBodyAndHeader(t, httpReq, req)
		response := executeTestRequest(t, httpReq, nil, testContext)
		assert.Equal(t, http.StatusBadRequest, response.Code)
	})

	// // Test invalid session creation
	tests := []struct {
		name        string
		sessionSpec string
		wantStatus  int
	}{
		{
			name: "missing skillPath",
			sessionSpec: `{
				"viewName": "valid-view",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test"
				}
			}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing viewName",
			sessionSpec: `{
				"skillPath": "/valid-skillset/test-skill",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test"
				}
			}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid skillPath",
			sessionSpec: `{
				"skillPath": "invalid/path",
				"viewName": "valid-view",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test"
				}
			}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non-existent view",
			sessionSpec: `{
				"skillPath": "/valid-skillset/test-skill",
				"viewName": "non-existent-view",
				"sessionVariables": {
					"key1": "value1"
				},
				"inputArgs": {
					"input": "test"
				}
			}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.Config().DefaultTenantID = string(tenantID)
			config.Config().DefaultProjectID = string(projectID)
			httpReq, _ := http.NewRequest("POST", "/sessions", nil)
			setRequestBodyAndHeader(t, httpReq, tt.sessionSpec)
			response := executeTestRequest(t, httpReq, nil, testContext)
			assert.Equal(t, tt.wantStatus, response.Code)
		})
	}
}
