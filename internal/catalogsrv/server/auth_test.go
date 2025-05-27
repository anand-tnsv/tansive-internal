package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/pkg/types"
)

type testSetup struct {
	ctx       context.Context
	tenantID  types.TenantId
	projectID types.ProjectId
}

func TestAdoptView(t *testing.T) {
	_ = setupTest(t)

	// Try to get Catalog without token
	httpReq, _ := http.NewRequest("GET", "/catalogs/test-catalog", nil)
	response := executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusUnauthorized, response.Code)

	// Test successful adoption of default view
	token := adoptDefaultView(t, "test-catalog")

	// Try to get Catalog with adopted token
	httpReq, _ = http.NewRequest("GET", "/catalogs/test-catalog", nil)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	// Setup test objects with the adopted token
	setupObjects(t, token)

	// Adopt the read-only view
	readOnlyToken := adoptView(t, "test-catalog", "read-only-view", token)

	// Try to get resource1 with read-only view token
	httpReq, _ = http.NewRequest("GET", "/resources/resource1", nil)
	httpReq.Header.Set("Authorization", "Bearer "+readOnlyToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	// Try to update resource1 with read-only view token - should fail
	httpReq, _ = http.NewRequest("PUT", "/resources/resource1", nil)
	req := `
		{
			"name": "resource1",
			"value": 100
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+readOnlyToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusForbidden, response.Code)

	// Adopt the read-write view
	readWriteToken := adoptView(t, "test-catalog", "read-write-view", token)

	// Try to get resource1 with read-write view token
	httpReq, _ = http.NewRequest("GET", "/resources/resource1", nil)
	httpReq.Header.Set("Authorization", "Bearer "+readWriteToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	// Try to update resource1 with read-write view token - should succeed
	httpReq, _ = http.NewRequest("PUT", "/resources/resource1", nil)
	req = `
		{
			"name": "resource1",
			"value": 200
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+readWriteToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	// Verify the update was successful
	httpReq, _ = http.NewRequest("GET", "/resources/resource1", nil)
	httpReq.Header.Set("Authorization", "Bearer "+readWriteToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	var resourceResponse struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}
	err := json.Unmarshal(response.Body.Bytes(), &resourceResponse)
	require.NoError(t, err)
	require.Equal(t, 200, resourceResponse.Value)

	// Try to update resource1 definition with read-write view token - should fail
	httpReq, _ = http.NewRequest("PUT", "/resources/resource1/definition", nil)
	req = `
		{
			"version": "v1",
			"kind": "Resource",
			"metadata": {
				"name": "resource1",
				"catalog": "test-catalog",
				"variant": "test-variant",
				"namespace": "",
				"path": "/",
				"description": "Updated test resource"
			},
			"spec": {
				"schema": {
					"type": "object",
					"properties": {
						"name": {
							"type": "string"
						},
						"value": {
							"type": "integer"
						}
					}
				},
				"value": {
					"name": "resource1",
					"value": 42
				},
				"annotations": null,
				"policy": ""
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+readWriteToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusForbidden, response.Code)

	// Adopt the full-access view
	fullAccessToken := adoptView(t, "test-catalog", "full-access-view", token)

	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+fullAccessToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	// Verify the definition update was successful
	httpReq, _ = http.NewRequest("GET", "/resources/resource1/definition", nil)
	httpReq.Header.Set("Authorization", "Bearer "+fullAccessToken)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	var definitionResponse struct {
		Metadata struct {
			Description string `json:"description"`
		} `json:"metadata"`
	}
	err = json.Unmarshal(response.Body.Bytes(), &definitionResponse)
	require.NoError(t, err)
	require.Equal(t, "Updated test resource", definitionResponse.Metadata.Description)
}

func setupTest(t *testing.T) *testSetup {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	cfg := config.Config()
	cfg.DefaultTenantID = string(tenantID)
	cfg.DefaultProjectID = string(projectID)

	// Set the tenant ID and project ID in the context
	ctx = catcommon.SetTenantIdInContext(ctx, tenantID)
	ctx = catcommon.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	require.NoError(t, err)

	// Create a catalog
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "test-catalog",
				"description": "Test catalog for adopt view"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	response := executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)

	return &testSetup{
		ctx:       ctx,
		tenantID:  tenantID,
		projectID: projectID,
	}
}

func setupObjects(t *testing.T, token string) {
	// Create a variant
	httpReq, _ := http.NewRequest("POST", "/variants", nil)
	req := `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "test-variant",
				"description": "Test variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response := executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)

	// Create two resources
	httpReq, _ = http.NewRequest("POST", "/resources", nil)
	req = `
		{
			"version": "v1",
			"kind": "Resource",
			"metadata": {
				"name": "resource1",
				"catalog": "test-catalog",
				"variant": "test-variant",
				"namespace": "",
				"path": "/",
				"description": "First test resource"
			},
			"spec": {
				"schema": {
					"type": "object",
					"properties": {
						"name": {
							"type": "string"
						},
						"value": {
							"type": "integer"
						}
					}
				},
				"value": {
					"name": "resource1",
					"value": 42
				},
				"annotations": null,
				"policy": ""
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)

	httpReq, _ = http.NewRequest("POST", "/resources", nil)
	req = `
		{
			"version": "v1",
			"kind": "Resource",
			"metadata": {
				"name": "resource2",
				"catalog": "test-catalog",
				"variant": "test-variant",
				"namespace": "",
				"path": "/",
				"description": "Second test resource"
			},
			"spec": {
				"schema": {
					"type": "object",
					"properties": {
						"name": {
							"type": "string"
						},
						"value": {
							"type": "integer"
						}
					}
				},
				"value": {
					"name": "resource2",
					"value": 100
				},
				"annotations": null,
				"policy": ""
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)

	// Create first view with resource.get permission
	httpReq, _ = http.NewRequest("POST", "/views", nil)
	req = `
		{
			"version": "v1",
			"kind": "View",
			"metadata": {
				"name": "read-only-view",
				"catalog": "test-catalog",
				"description": "View with read-only access"
			},
			"spec": {
				"definition": {
					"scope": {
						"catalog": "test-catalog",
						"variant": "test-variant"
					},
					"rules": [{
						"intent": "Allow",
						"actions": ["resource.get"],
						"targets": ["res://resources/*"]
					}]
				}
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)

	// Create second view with resource.get and resource.put permissions
	httpReq, _ = http.NewRequest("POST", "/views", nil)
	req = `
		{
			"version": "v1",
			"kind": "View",
			"metadata": {
				"name": "read-write-view",
				"catalog": "test-catalog",
				"description": "View with read and write access"
			},
			"spec": {
				"definition": {
					"scope": {
						"catalog": "test-catalog",
						"variant": "test-variant"
					},
					"rules": [{
						"intent": "Allow",
						"actions": ["resource.get", "resource.put"],
						"targets": ["res://resources/*"]
					}]
				}
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)

	// Create third view with full resource permissions
	httpReq, _ = http.NewRequest("POST", "/views", nil)
	req = `
		{
			"version": "v1",
			"kind": "View",
			"metadata": {
				"name": "full-access-view",
				"catalog": "test-catalog",
				"description": "View with full resource access"
			},
			"spec": {
				"definition": {
					"scope": {
						"catalog": "test-catalog",
						"variant": "test-variant"
					},
					"rules": [{
						"intent": "Allow",
						"actions": ["resource.get", "resource.put", "resource.edit"],
						"targets": ["res://resources/*"]
					}]
				}
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response = executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusCreated, response.Code)
}

func adoptDefaultView(t *testing.T, catalog string) string {
	httpReq, _ := http.NewRequest("POST", "/auth/adopt-default-view/"+catalog, nil)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	response := executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	var adoptResponse struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	err := json.Unmarshal(response.Body.Bytes(), &adoptResponse)
	require.NoError(t, err)
	require.NotEmpty(t, adoptResponse.Token)
	require.NotEmpty(t, adoptResponse.ExpiresAt)

	return adoptResponse.Token
}

func adoptView(t *testing.T, catalog, viewLabel, token string) string {
	httpReq, _ := http.NewRequest("POST", "/auth/adopt-view/"+catalog+"/"+viewLabel, nil)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response := executeTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	var adoptResponse struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	err := json.Unmarshal(response.Body.Bytes(), &adoptResponse)
	require.NoError(t, err)
	require.NotEmpty(t, adoptResponse.Token)
	require.NotEmpty(t, adoptResponse.ExpiresAt)

	return adoptResponse.Token
}
