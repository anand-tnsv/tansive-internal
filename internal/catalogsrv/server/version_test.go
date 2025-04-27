package server

import (
	"net/http"
	"testing"

	"github.com/tansive/tansive-internal/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestGetVersion(t *testing.T) {
	// Create a New Request
	req, _ := http.NewRequest("GET", "/version", nil)
	testContext := TestContext{
		TenantId:  "tenant1",
		ProjectId: "project1",
	}
	// Execute Request
	response := executeTestRequest(t, req, nil, testContext)

	// Check the response code
	require.Equal(t, http.StatusOK, response.Code)

	// Check headers
	checkHeader(t, response.Result().Header)

	compareJson(t,
		&api.GetVersionRsp{
			ServerVersion: "CatalogSrv: 1.0.0", //TODO - Implement server versioning
			ApiVersion:    api.ApiVersion_1_0,
		}, response.Body.String())
}
