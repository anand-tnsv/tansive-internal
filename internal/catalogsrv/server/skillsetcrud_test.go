package server

import (
	"net/http"
	"testing"

	json "github.com/json-iterator/go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
)

func TestSkillSetCrud(t *testing.T) {
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
				"annotations": null,
				"runner": {
					"id": "system.commandrunner",
					"config": {
						"command": "python3 test.py"
					}
				},
				"context": [
					{
						"name": "test-context",
						"provider": {},
						"schema": {
							"type": "object",
							"properties": {
								"name": {
									"type": "string"
								}
							}
						},
						"value": null
					}
				],
				"skills": [
					{
						"name": "test-skill",
						"description": "Test skill",
						"annotations": null,
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
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.Contains(t, response.Header().Get("Location"), "/skillsets/valid-skillset")

	// Get the skillset
	httpReq, _ = http.NewRequest("GET", "/skillsets/valid-skillset", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	rspType := make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType := make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Update the skillset
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
				"description": "This is an updated skillset"
			},
			"spec": {
				"version": "1.0.0",
				"annotations": null,
				"runner": {
					"id": "system.commandrunner",
					"config": {
						"command": "python3 updated_test.py"
					}
				},
				"context": [
					{
						"name": "updated-context",
						"provider": {},
						"schema": {
							"type": "object",
							"properties": {
								"name": {
									"type": "string"
								}
							}
						},
						"value": null
					}
				],
				"skills": [
					{
						"name": "updated-skill",
						"description": "Updated skill",
						"annotations": null,
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
						"exportedActions": ["updated.action"]
					}
				],
				"dependencies": [
					{
						"path": "/resources/updated",
						"kind": "Resource",
						"alias": "updated-resource",
						"actions": ["read"]
					}
				]
			}
		}`
	httpReq, _ = http.NewRequest("PUT", "/skillsets/valid-skillset", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get the updated skillset
	httpReq, _ = http.NewRequest("GET", "/skillsets/valid-skillset", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	rspType = make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType = make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Delete the skillset
	httpReq, _ = http.NewRequest("DELETE", "/skillsets/valid-skillset", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Try to get the deleted skillset
	httpReq, _ = http.NewRequest("GET", "/skillsets/valid-skillset", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Try to update non-existing skillset
	httpReq, _ = http.NewRequest("PUT", "/skillsets/not-existing-skillset", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestSkillSetList(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("PABCDE")

	ctx = catcommon.WithTenantID(ctx, tenantID)
	ctx = catcommon.WithProjectID(ctx, projectID)

	config.Config().DefaultProjectID = string(projectID)
	config.Config().DefaultTenantID = string(tenantID)

	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})
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
				"name": "list-catalog",
				"description": "Catalog for skillset list test"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().Auth.FakeSingleUserToken)
	response := executeTestRequest(t, httpReq, nil, testContext)
	assert.Equal(t, http.StatusCreated, response.Code)
	testContext.CatalogContext.Catalog = "list-catalog"

	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "list-variant",
				"description": "Variant for skillset list test"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	assert.Equal(t, http.StatusCreated, response.Code)
	testContext.CatalogContext.Variant = "list-variant"

	// Create skillsets
	skillsets := []struct {
		Name        string
		Description string
		Command     string
	}{
		{
			"skillset1",
			"First test skillset",
			"python3 test1.py",
		},
		{
			"skillset2",
			"Second test skillset",
			"python3 test2.py",
		},
		{
			"internal",
			"Internal skillset",
			"python3 internal.py",
		},
	}

	for _, s := range skillsets {
		req = `
		{
			"version": "v1",
			"kind": "SkillSet",
			"metadata": {
				"name": "` + s.Name + `",
				"catalog": "list-catalog",
				"variant": "list-variant",
				"description": "` + s.Description + `"
			},
			"spec": {
				"version": "1.0.0",
				"annotations": null,
				"runner": {
					"id": "system.commandrunner",
					"config": {
						"command": "` + s.Command + `"
					}
				},
				"context": [
					{
						"name": "test-context",
						"provider": {},
						"schema": {
							"type": "object",
							"properties": {
								"name": {
									"type": "string"
								}
							}
						},
						"value": null
					}
				],
				"skills": [
					{
						"name": "test-skill",
						"description": "Test skill",
						"annotations": null,
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
		}`
		httpReq, _ = http.NewRequest("POST", "/skillsets", nil)
		setRequestBodyAndHeader(t, httpReq, req)
		response = executeTestRequest(t, httpReq, nil, testContext)
		t.Logf("Response: %v", response.Body.String())
		require.Equal(t, http.StatusCreated, response.Code)
	}

	// List skillsets
	httpReq, _ = http.NewRequest("GET", "/skillsets?catalog=list-catalog&variant=list-variant", nil)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().Auth.FakeSingleUserToken)
	response = executeTestRequest(t, httpReq, nil, testContext)
	require.Equal(t, http.StatusOK, response.Code)

	var result = make(map[string]json.RawMessage)
	err = json.Unmarshal(response.Body.Bytes(), &result)
	assert.NoError(t, err)

	// All skillsets should be present
	assert.Len(t, result, 3)
}
