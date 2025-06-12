package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	srvsession "github.com/tansive/tansive-internal/internal/catalogsrv/session"
	"github.com/tansive/tansive-internal/internal/tangent/runners/stdiorunner"
	"github.com/tansive/tansive-internal/internal/tangent/session"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
	"github.com/tansive/tansive-internal/internal/tangent/test"
)

func TestHandleInteractiveSession(t *testing.T) {
	session.SetTestMode(true)
	stdiorunner.TestInit()
	ts := test.SetupTestCatalog(t)
	token := test.AdoptDefaultView(t, ts.Catalog)
	t.Logf("Token: %s", token)

	sessionReq := createInteractiveSession(t, token)
	sessionReq.Interactive = true

	httpReq, _ := http.NewRequest("POST", "/sessions", nil)
	setRequestBodyAndHeader(t, httpReq, sessionReq)
	response := executeTestRequest(t, httpReq, nil)
	t.Logf("Response: %s", response.Body.String())
	require.Equal(t, http.StatusOK, response.Code)
}

func createInteractiveSession(t *testing.T, token string) *tangentcommon.SessionCreateRequest {
	codeVerifier := "test_challenge"
	hashed := sha256.Sum256([]byte(codeVerifier))
	codeChallenge := base64.RawURLEncoding.EncodeToString(hashed[:])
	httpReq, _ := http.NewRequest("POST", "/sessions?interactive=true&code_challenge="+codeChallenge, nil)
	req := `
		{
			"skillPath": "/skillsets/kubernetes-demo/k8s_troubleshooter",
			"viewName": "dev-view",
			"sessionVariables": {
				"key1": "value1",
				"key2": 123,
				"key3": true
			},
			"inputArgs": {
				"prompt": "My order system is not working. Can you use the tools you have to fix it?"
			}
		}`
	test.SetRequestBodyAndHeader(t, httpReq, req)
	// add the token to the header
	httpReq.Header.Set("Authorization", "Bearer "+token)
	response := test.ExecuteTestRequest(t, httpReq, nil)
	require.Equal(t, http.StatusOK, response.Code)

	var sessionResp srvsession.InteractiveSessionRsp
	err := json.Unmarshal(response.Body.Bytes(), &sessionResp)
	require.NoError(t, err)
	require.NotEmpty(t, sessionResp.Code)
	require.NotEmpty(t, sessionResp.TangentURL)
	return &tangentcommon.SessionCreateRequest{
		Code:         sessionResp.Code,
		CodeVerifier: codeVerifier,
	}
}
