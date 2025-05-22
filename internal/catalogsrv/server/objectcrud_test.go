package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"sigs.k8s.io/yaml"
)

func TestCreateTenantProject(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TANAND")
	projectID := types.ProjectId("PANAND")

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	if err == nil {
		t.Logf("Tenant created successfully")
	} else {
		t.Logf("Tenant creation failed: %v", err)
	}

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	if err == nil {
		t.Logf("Project created successfully")
	} else {
		t.Logf("Project creation failed: %v", err)
	}
}

func TestCatalogCreate(t *testing.T) {

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
		TenantId:  tenantID,
		ProjectId: projectID,
	}

	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)

	req := `
{
	"version": "v1",
	"kind": "Catalog",
	"metadata": {
		"name": "valid-catalog",
		"description": "This is a valid catalog"
	}
} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Check Location in header
	assert.Contains(t, response.Header().Get("Location"), "/catalogs/valid-catalog")
}

func TestGetUpdateDeleteCatalog(t *testing.T) {
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
		TenantId:  tenantID,
		ProjectId: projectID,
	}

	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)

	req := `
{
	"version": "v1",
	"kind": "Catalog",
	"metadata": {
		"name": "valid-catalog",
		"description": "This is a valid catalog"
	}
} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Catalog = "valid-catalog"
	// Create a New Request to get the catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
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

	// Create a New Request to get a non-existing catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/validcatalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	t.Logf("Response: %v", response.Body.String())
	require.NotEqual(t, http.StatusOK, response.Code)

	// Update the catalog
	req = `
{
	"version": "v1",
	"kind": "Catalog",
	"metadata": {
		"name": "valid-catalog",
		"description": "This is a new description"
	}
} `
	httpReq, _ = http.NewRequest("PUT", "/catalogs/valid-catalog", nil)
	setRequestBodyAndHeader(t, httpReq, req)

	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
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

	// Delete the catalog
	httpReq, _ = http.NewRequest("DELETE", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the deleted catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	require.NotEqual(t, http.StatusOK, response.Code)
}

func TestVariantCrud(t *testing.T) {
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
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
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
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Check Location in header
	assert.Contains(t, response.Header().Get("Location"), "/variants")
	loc := response.Header().Get("Location")

	// Get the variant
	httpReq, _ = http.NewRequest("GET", loc, nil)
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

	// Create a new variant on the /variants endpoint
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant2",
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Check Location in header
	assert.Contains(t, response.Header().Get("Location"), "/variants/")
	loc = response.Header().Get("Location")
	// Get the variant
	httpReq, _ = http.NewRequest("GET", loc+"?c=valid-catalog", nil)
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

	// Create a new variant by updating the testcontext
	testContext.CatalogContext.Catalog = "invalid-catalog"
	req, _ = sjson.Set(req, "metadata.variant", "valid-variant-3")
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusBadRequest, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Update the variant
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"catalog": "valid-catalog",
				"description": "This is a new description"
			}
		}`
	testContext.CatalogContext.Catalog = "valid-catalog"
	httpReq, _ = http.NewRequest("PUT", "/variants/valid-variant", nil)
	setRequestBodyAndHeader(t, httpReq, req)

	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the variant
	httpReq, _ = http.NewRequest("GET", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
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

	// Delete the variant
	httpReq, _ = http.NewRequest("DELETE", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the deleted variant
	httpReq, _ = http.NewRequest("GET", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get the variant
	httpReq, _ = http.NewRequest("GET", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestNamespaceCrud(t *testing.T) {
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
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
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
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a namespace
	httpReq, _ = http.NewRequest("POST", "/namespaces?c=valid-catalog&v=valid-variant", nil)
	req = `
		{
			"version": "v1",
			"kind": "Namespace",
			"metadata": {
				"name": "valid-namespace",
				"description": "This is a valid namespace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Check Location in header
	assert.Equal(t, "/namespaces/valid-namespace", response.Header().Get("Location"))
	// Get the namespace
	httpReq, _ = http.NewRequest("GET", "/namespaces/valid-namespace?v=valid-variant&c=valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestWorkspaceCrud(t *testing.T) {
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
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Catalog = "valid-catalog"
	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/variants?catalog=valid-catalog", nil)
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
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Create a workspace
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces?c=valid-catalog&v=valid-variant", nil)
	req = `
		{
			"version": "v1",
			"kind": "Workspace",
			"metadata": {
				"name": "valid-workspace",
				"description": "This is a valid workspace"
			}
	}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc := response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/workspaces/valid-workspace", loc)

	// get this workspace
	testContext.CatalogContext.Catalog = "valid-catalog"
	testContext.CatalogContext.Variant = "valid-variant"
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	// update the workspace
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"name": "valid-workspace",
			"description": "This is a new description"
		}
	}`
	testContext.CatalogContext.Catalog = "valid-catalog"
	testContext.CatalogContext.Variant = "valid-variant"
	httpReq, _ = http.NewRequest("PUT", "/workspaces/valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	// Delete the workspace
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a workspace without a name
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces", nil)
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"description": "This is a valid workspace"
		}
	}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	require.NotEqual(t, http.StatusCreated, response.Code)

	// Set the name to empty
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"name": "",
			"description": ""
		}
	}`
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	assert.NotEqual(t, http.StatusOK, response.Code)

	// Delete the workspace
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete again
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

}

func TestObjectCrud(t *testing.T) {
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
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Create a variant
	testContext.CatalogContext.Catalog = "valid-catalog"
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
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Variant = "valid-variant"

	// create a namespace
	httpReq, _ = http.NewRequest("POST", "/namespaces?c=valid-catalog", nil)
	req = `
		{
			"version": "v1",
			"kind": "Namespace",
			"metadata": {
				"name": "valid-namespace",
				"description": "This is a valid namespace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Create a workspace
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces", nil)
	req = `
		{
			"version": "v1",
			"kind": "Workspace",
			"metadata": {
				"name": "valid-workspace",
				"description": "This is a valid workspace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc := response.Header().Get("Location")
	assert.NotEmpty(t, loc)

	// Create an object
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/collectionschemas", nil)
	reqYaml := `
			version: v1
			kind: CollectionSchema
			metadata:
				name: valid
				path: /
				description: This is a valid collection
			spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err := yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)

	// Get the object
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson := response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a valid collection")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec").String(), "{}")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")

	// Update the object
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a valid collection
		spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update just description
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a new description")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec").String(), "{}")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")

	// update a non-existing collection
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/invalid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	reqJson, _ = sjson.SetBytes(reqJson, "spec.parameters.garbage", "true")
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusBadRequest, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update the collection with a dataType
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec:
			parameters:
				maxDelay:
					schema: ""
					dataType: Integer
					default: 1000
					annotations:
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	// compare the spec
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.schema").String(), "")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.dataType").String(), "Integer")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.default").String(), "1000")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.annotations").String(), "")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a new description")

	// send the same update request to the namespace
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid?n=valid-namespace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// create the schema in the namespace
	httpReq, _ = http.NewRequest("POST", "/collectionschemas?n=valid-namespace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collectionschemas/valid?namespace=valid-namespace", loc)

	// Get the object
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "valid-namespace")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a new description")

	// create a valid parameter
	reqYaml = `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: valid-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
			`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	paramReqJson := reqJson
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("POST", "/parameterschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/parameterschemas/integer-param-schema?namespace=valid-namespace&workspace=valid-workspace", loc)

	// Get the parameter
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// add this parameter to root
	httpReq, _ = http.NewRequest("POST", "/parameterschemas", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update the collection with an the newly created parameter
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec:
			parameters:
				maxDelay:
					schema: ""
					dataType: Integer
					default: 1000
					annotations:
				maxRetries:
					schema: integer-param-schema
					dataType: ""
					default: 8
					annotations:
	`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)

	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// add this collection to workspace and namespace
	httpReq, _ = http.NewRequest("POST", "/collectionschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// modify the parameter to a smaller max value
	reqYaml = `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: valid-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
			`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/parameterschemas/integer-param-schema", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.NotEqual(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Test collections
	reqYaml = `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			path: /some/random/path
		spec:
			schema: valid
			values:
				maxRetries: 3
				maxAttempts: 10
	`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("POST", "/collections", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collections/some/random/path/my-collection", loc)

	// create it again
	httpReq, _ = http.NewRequest("POST", "/collections", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusConflict, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// get the collection
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "my-collection")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/some/random/path")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.schema").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxRetries").String(), "3")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxAttempts").String(), "10")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")

	// update the collection
	reqJson, _ = sjson.SetBytes(reqJson, "spec.values.maxRetries", 5)
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// get the collection
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "my-collection")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/some/random/path")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.schema").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxRetries").String(), "5")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxAttempts").String(), "10")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")

	// update the collection with a non-existing schema
	collReqJson := reqJson
	reqJson, _ = sjson.SetBytes(reqJson, "spec.schema", "invalid")
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusBadRequest, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update a non existing collection
	httpReq, _ = http.NewRequest("PUT", "/collections/invalid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// delete the collection
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted collection
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// delete the collection schema
	httpReq, _ = http.NewRequest("DELETE", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted collection
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// create the collection in a namespace and workspace
	httpReq, _ = http.NewRequest("POST", "/collections?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(collReqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collections/some/random/path/my-collection?namespace=valid-namespace&workspace=valid-workspace", loc)

	// delete the parameter
	httpReq, _ = http.NewRequest("DELETE", "/parameterschemas/integer-param-schema", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted parameter
	httpReq, _ = http.NewRequest("GET", "/parameterschemas/integer-param-schema", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// create the parameter in the namespace again
	httpReq, _ = http.NewRequest("POST", "/parameterschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(paramReqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusConflict, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete the collection
	httpReq, _ = http.NewRequest("DELETE", "/collections/some/random/path/my-collection?namespace=valid-namespace&workspace=valid-workspace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete the collection schema
	httpReq, _ = http.NewRequest("DELETE", "/collectionschemas/valid?namespace=valid-namespace&workspace=valid-workspace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// delete the parameter in the namespace
	httpReq, _ = http.NewRequest("DELETE", "/parameterschemas/integer-param-schema?workspace=valid-workspace&namespace=valid-namespace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted parameter
	httpReq, _ = http.NewRequest("GET", "/parameterschemas/integer-param-schema?workspace=valid-workspace&namespace=valid-namespace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

}

func TestValuesCrud(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})
	testContextP := createTestObjects(t, ctx)
	testContext := *testContextP
	assert.NotEmpty(t, testContext)

	// Create a new Collection
	reqYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			path: /some/random/path
		spec:
			schema: valid
			values:
				maxValue: 100
				maxLength: 10
	`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err := yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ := http.NewRequest("POST", "/collections?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response := executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc := response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collections/some/random/path/my-collection?namespace=valid-namespace&workspace=valid-workspace", loc)

	// Get a parameter
	// get the location string before ?
	col_loc := loc[:strings.LastIndex(loc, "?")]
	col_loc = strings.Replace(col_loc, "/collections", "/attributes", 1)
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxRetries?namespace=valid-namespace&workspace=valid-workspace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson := response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.NotEmpty(t, rspJson)
	checkHeader(t, response.Header())
	assert.Equal(t, gjson.GetBytes(rspJson, "maxRetries").Exists(), true)
	assert.Equal(t, gjson.GetBytes(rspJson, "maxRetries.value").String(), "5")

	testContext.CatalogContext.Namespace = "valid-namespace"
	testContext.CatalogContext.WorkspaceLabel = "valid-workspace"
	// Get max attempts
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxAttempts", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.True(t, gjson.GetBytes(rspJson, "maxAttempts").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "maxAttempts.value").String(), "8")
	assert.Equal(t, gjson.GetBytes(rspJson, "maxAttempts.annotations.env").String(), "MAX_ATTEMPTS")

	// Get max delay
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxDelay", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.True(t, gjson.GetBytes(rspJson, "maxDelay").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "maxDelay.value").String(), "1000")
	assert.Equal(t, gjson.GetBytes(rspJson, "maxDelay.annotations").String(), "")

	// Get max value
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxValue", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.True(t, gjson.GetBytes(rspJson, "maxValue").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "maxValue.value").String(), "100")
	assert.Equal(t, gjson.GetBytes(rspJson, "maxValue.annotations.env").String(), "MAX_VALUE")

	// Get max length
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxLength", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.True(t, gjson.GetBytes(rspJson, "maxLength").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "maxLength.value").String(), "10")
	assert.Equal(t, gjson.GetBytes(rspJson, "maxLength.annotations.env").String(), "MAX_LENGTH")

	// Update the max length
	updateJson := `{"value": 9}`
	httpReq, _ = http.NewRequest("POST", col_loc+"/maxLength", nil)
	setRequestBodyAndHeader(t, httpReq, updateJson)
	response = executeTestRequest(t, httpReq, nil, testContext)

	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get max length
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxLength", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()

	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	assert.True(t, gjson.GetBytes(rspJson, "maxLength").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "maxLength.value").String(), "9")
	assert.Equal(t, gjson.GetBytes(rspJson, "maxLength.annotations.env").String(), "MAX_LENGTH")

	// Update multiple values
	updateJson = `{"values": {"maxValue": 200, "maxLength": 6}}`
	httpReq, _ = http.NewRequest("POST", col_loc, nil)
	setRequestBodyAndHeader(t, httpReq, updateJson)
	response = executeTestRequest(t, httpReq, nil, testContext)

	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get all values in collection
	httpReq, _ = http.NewRequest("GET", col_loc+"?collection=true", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	assert.True(t, gjson.GetBytes(rspJson, "values.maxValue").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "values.maxValue.value").String(), "200")
	assert.Equal(t, gjson.GetBytes(rspJson, "values.maxValue.annotations.env").String(), "MAX_VALUE")
	assert.True(t, gjson.GetBytes(rspJson, "values.maxLength").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "values.maxLength.value").String(), "6")
	assert.Equal(t, gjson.GetBytes(rspJson, "values.maxLength.annotations.env").String(), "MAX_LENGTH")

	// Delete the max length
	httpReq, _ = http.NewRequest("DELETE", col_loc+"/maxLength", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted max length
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxLength", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// it should be equal to default value
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "maxLength").Exists(), true)
	assert.Equal(t, gjson.GetBytes(rspJson, "maxLength.value").String(), "8")

	// Delete the max value
	httpReq, _ = http.NewRequest("DELETE", col_loc+"/maxValue", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted max value
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxValue", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// it should be equal to default value
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "maxValue").Exists(), true)
	assert.Equal(t, gjson.GetBytes(rspJson, "maxValue.value").String(), "10")

	// delete maxRetries
	httpReq, _ = http.NewRequest("DELETE", col_loc+"/maxRetries", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted maxRetries
	httpReq, _ = http.NewRequest("GET", col_loc+"/maxRetries", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// it should be equal to default value
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "maxRetries").Exists(), true)
	assert.Equal(t, gjson.GetBytes(rspJson, "maxRetries.value").String(), "5")

	// get all values in collection
	httpReq, _ = http.NewRequest("GET", col_loc+"?collection=true", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	rspJson = response.Body.Bytes()
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	t.Logf("Response: %v", response.Body.String())
	assert.True(t, gjson.GetBytes(rspJson, "values.maxRetries").Exists())
	assert.Equal(t, gjson.GetBytes(rspJson, "values.maxRetries.value").String(), "5")
}

func createTestObjects(t *testing.T, ctx context.Context) *TestContext {
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

	testContext := TestContext{
		TenantId:  tenantID,
		ProjectId: projectID,
	}

	// Create a catalog
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Create a variant
	testContext.CatalogContext.Catalog = "valid-catalog"
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
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	testContext.CatalogContext.Variant = "valid-variant"

	// create a namespace
	httpReq, _ = http.NewRequest("POST", "/namespaces?c=valid-catalog", nil)
	req = `
		{
			"version": "v1",
			"kind": "Namespace",
			"metadata": {
				"name": "valid-namespace",
				"description": "This is a valid namespace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	//	testContext.CatalogContext.Namespace = "valid-namespace"

	// Create a workspace
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces", nil)
	req = `
		{
			"version": "v1",
			"kind": "Workspace",
			"metadata": {
				"name": "valid-workspace",
				"description": "This is a valid workspace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc := response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	//	testContext.CatalogContext.WorkspaceLabel = "valid-workspace"

	// create a valid parameter
	reqYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: valid-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5		
			`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err := yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("POST", "/parameterschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/parameterschemas/integer-param-schema?namespace=valid-namespace&workspace=valid-workspace", loc)

	// update the collection with an the newly created parameter
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec:
			parameters:
				maxDelay:
					schema: ""
					dataType: Integer
					default: 1000
					annotations:
				maxRetries:
					schema: integer-param-schema
					dataType: ""
					annotations:
				maxAttempts:
					schema: integer-param-schema
					dataType: ""
					default: 8
					annotations:
						env: MAX_ATTEMPTS
				maxLength:
					schema: integer-param-schema
					dataType: ""
					default: 8
					annotations:
						env: MAX_LENGTH
						maxValue: 10
				maxValue:
					dataType: Integer
					default: 10
					annotations:
						env: MAX_VALUE
	`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)

	httpReq, _ = http.NewRequest("POST", "/collectionschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	return &testContext
}
func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}
