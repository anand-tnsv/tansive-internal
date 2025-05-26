package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestResourceCrud(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	config.Config().DefaultProjectID = string(projectID)
	config.Config().DefaultTenantID = string(tenantID)

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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
		CatalogContext: common.CatalogContext{},
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
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
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
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Variant = "valid-variant"

	// Create a resource
	httpReq, _ = http.NewRequest("POST", "/resources", nil)
	req = `
		{
			"version": "v1",
			"kind": "Resource",
			"metadata": {
				"name": "valid-resource",
				"catalog": "valid-catalog",
				"variant": "valid-variant",
				"namespace": "",
				"path": "/",
				"description": "This is a valid resource"
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
					"name": "test-resource",
					"value": 42
				},
				"annotations": null,
				"policy": ""
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.Contains(t, response.Header().Get("Location"), "/resources/valid-resource")

	// Get the resource
	httpReq, _ = http.NewRequest("GET", "/resources/valid-resource/definition", nil)
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

	// Update the resource
	req = `
		{
			"version": "v1",
			"kind": "Resource",
			"metadata": {
				"name": "valid-resource",
				"catalog": "valid-catalog",
				"variant": "valid-variant",
				"namespace": "",
				"path": "/",
				"description": "This is an updated resource"
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
					"name": "updated-resource",
					"value": 100
				},
				"annotations": null,
				"policy": ""
			}
		}`
	httpReq, _ = http.NewRequest("PUT", "/resources/valid-resource/definition", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get the updated resource
	httpReq, _ = http.NewRequest("GET", "/resources/valid-resource/definition", nil)
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

	// Delete the resource
	httpReq, _ = http.NewRequest("DELETE", "/resources/valid-resource/definition", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Try to get the deleted resource
	httpReq, _ = http.NewRequest("GET", "/resources/valid-resource/definition", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Try to update non-existing resource
	httpReq, _ = http.NewRequest("PUT", "/resources/not-existing-resource/definition", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestResourceList(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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
		CatalogContext: common.CatalogContext{},
	}

	// Create a catalog
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "list-catalog",
				"description": "Catalog for resource list test"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
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
				"description": "Variant for resource list test"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	assert.Equal(t, http.StatusCreated, response.Code)
	testContext.CatalogContext.Variant = "list-variant"

	// Create resources
	resources := []struct {
		Name        string
		Description string
		Value       map[string]interface{}
	}{
		{
			"resource1",
			"First test resource",
			map[string]interface{}{
				"name":  "test1",
				"value": 1,
			},
		},
		{
			"resource2",
			"Second test resource",
			map[string]interface{}{
				"name":  "test2",
				"value": 2,
			},
		},
		{
			"internal",
			"Internal resource",
			map[string]interface{}{
				"name":  "internal",
				"value": 3,
			},
		},
	}

	for _, r := range resources {
		req = `
		{
			"version": "v1",
			"kind": "Resource",
			"metadata": {
				"name": "` + r.Name + `",
				"catalog": "list-catalog",
				"variant": "list-variant",
				"description": "` + r.Description + `"
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
					"name": "` + r.Value["name"].(string) + `",
					"value": ` + strconv.Itoa(r.Value["value"].(int)) + `
				}
			}
		}`
		httpReq, _ = http.NewRequest("POST", "/resources", nil)
		setRequestBodyAndHeader(t, httpReq, req)
		response = executeTestRequest(t, httpReq, nil, testContext)
		assert.Equal(t, http.StatusCreated, response.Code)
	}

	// List resources
	httpReq, _ = http.NewRequest("GET", "/resources?catalog=list-catalog&variant=list-variant", nil)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	response = executeTestRequest(t, httpReq, nil, testContext)
	require.Equal(t, http.StatusOK, response.Code)

	var result = make(map[string]json.RawMessage)
	err = json.Unmarshal(response.Body.Bytes(), &result)
	assert.NoError(t, err)

	// All resources should be present
	assert.Len(t, result, 3)
}
