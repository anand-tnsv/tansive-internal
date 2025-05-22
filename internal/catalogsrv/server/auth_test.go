package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestAdoptView(t *testing.T) {
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
				"name": "test-catalog",
				"description": "Test catalog for adopt view"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	response := executeTestRequest(t, httpReq, nil, testContext)
	assert.Equal(t, http.StatusCreated, response.Code)
	testContext.CatalogContext.Catalog = "test-catalog"

	// Create target view
	httpReq, _ = http.NewRequest("POST", "/views", nil)
	req = `
		{
			"version": "v1",
			"kind": "View",
			"metadata": {
				"name": "target-view",
				"catalog": "test-catalog",
				"description": "Target view for adoption"
			},
			"spec": {
				"definition": {
					"scope": {
						"catalog": "test-catalog"
					},
					"rules": [{
						"intent": "Allow",
						"actions": ["catalog.list"],
						"targets": ["res://catalogs/test-catalog"]
					}]
				}
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	assert.Equal(t, http.StatusCreated, response.Code)

	// Set up the current view context
	// sourceViewDef := &catalogmanager.ViewDefinition{
	// 	Scope: catalogmanager.Scope{
	// 		Catalog: "test-catalog",
	// 	},
	// 	Rules: []catalogmanager.Rule{
	// 		{
	// 			Intent: catalogmanager.IntentAllow,
	// 			Actions: []catalogmanager.Action{
	// 				catalogmanager.ActionCatalogList,
	// 			},
	// 			Targets: []catalogmanager.TargetResource{
	// 				"res://catalogs/test-catalog",
	// 			},
	// 		},
	// 	},
	// }
	//testContext.CatalogContext.ViewDefinition = sourceViewDef

	// Try to get Catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/test-catalog", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil)
	assert.Equal(t, http.StatusUnauthorized, response.Code)

	// Test successful adoption
	httpReq, _ = http.NewRequest("POST", "/auth/adopt-default-view/test-catalog", nil)
	// set bearer token
	httpReq.Header.Set("Authorization", "Bearer "+config.Config().FakeSingleUserToken)
	response = executeTestRequest(t, httpReq, nil)
	assert.Equal(t, http.StatusOK, response.Code)

	var adoptResponse struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	err = json.Unmarshal(response.Body.Bytes(), &adoptResponse)
	assert.NoError(t, err)
	assert.NotEmpty(t, adoptResponse.Token)
	assert.NotEmpty(t, adoptResponse.ExpiresAt)

	// Try to get Catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/test-catalog", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	httpReq.Header.Set("Authorization", "Bearer "+adoptResponse.Token)
	response = executeTestRequest(t, httpReq, nil, testContext)
	assert.Equal(t, http.StatusOK, response.Code)

	// // Test invalid catalog
	// httpReq, _ = http.NewRequest("POST", "/catalogs/invalid-catalog/views/target-view/adopt", nil)
	// response = executeTestRequest(t, httpReq, nil, testContext)
	// assert.Equal(t, http.StatusBadRequest, response.Code)

	// // Test invalid view
	// httpReq, _ = http.NewRequest("POST", "/catalogs/test-catalog/views/invalid-view/adopt", nil)
	// response = executeTestRequest(t, httpReq, nil, testContext)
	// assert.Equal(t, http.StatusBadRequest, response.Code)

	// // Test view from different catalog
	// httpReq, _ = http.NewRequest("POST", "/catalogs/other-catalog/views/target-view/adopt", nil)
	// response = executeTestRequest(t, httpReq, nil, testContext)
	// assert.Equal(t, http.StatusBadRequest, response.Code)
}
