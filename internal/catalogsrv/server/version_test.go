package server

import (
	"net/http"
	"testing"

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
		&GetVersionRsp{
			ServerVersion: "Tansive Catalog Server: 0.1.0", //TODO - Implement server versioning
			ApiVersion:    "v1alpha1",
		}, response.Body.String())
}
