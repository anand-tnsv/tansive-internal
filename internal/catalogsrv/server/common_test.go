package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog/log"

	"github.com/stretchr/testify/assert"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/pkg/types"
)

type TestContext struct {
	TenantId  types.TenantId
	ProjectId types.ProjectId
	catcommon.CatalogContext
}

func executeTestRequest(t *testing.T, req *http.Request, apiKey *string, testContext ...TestContext) *httptest.ResponseRecorder {
	s, err := CreateNewServer()
	assert.NoError(t, err, "create new server")

	if apiKey != nil {
		_ = apiKey
		//auth.SignApiRequest(req, apiKey.KeyId, apiKey.PrivKey)
	}

	// Mount Handlers
	s.MountHandlers()

	rr := httptest.NewRecorder()
	if len(testContext) > 0 {
		ctx := req.Context()
		ctx = catcommon.SetTenantIdInContext(ctx, testContext[0].TenantId)
		ctx = catcommon.SetProjectIdInContext(ctx, testContext[0].ProjectId)
		catalogContext := &testContext[0].CatalogContext
		vd := types.ViewDefinition{
			Scope: types.Scope{
				Catalog: testContext[0].CatalogContext.Catalog,
			},
			Rules: []types.Rule{
				{
					Intent:  types.IntentAllow,
					Actions: []types.Action{types.ActionCatalogAdmin},
					Targets: []types.TargetResource{types.TargetResource("res://catalogs/" + testContext[0].CatalogContext.Catalog)},
				},
			},
		}
		catalogContext.ViewDefinition = &vd
		ctx = catcommon.SetCatalogContext(ctx, catalogContext)
		ctx = catcommon.SetTestContext(ctx, true)
		req = req.WithContext(ctx)
	}
	s.Router.ServeHTTP(rr, req)

	return rr
}

func checkHeader(t *testing.T, h http.Header) {
	expected := "application/json"
	got := h.Get("Content-Type")
	assert.Equal(t, expected, got, "Content-Type expected %s, got %s", expected, got)
	assert.NotNil(t, h.Get("X-Request-ID"), "No Request Id")
}

func compareJson(t *testing.T, expected any, actual string) {
	j, err := json.Marshal(expected)
	assert.NoError(t, err, "json marshal")
	assert.JSONEq(t, string(j), actual, "Expected: %v\n Got: %v\n", expected, actual)
}

var _ = setRequestBodyAndHeader

func setRequestBodyAndHeader(t *testing.T, req *http.Request, data interface{}) {
	// Marshal the data into JSON
	// check if the input itsef is json
	var jsonData []byte
	if s, ok := data.(string); ok {
		if json.Valid([]byte(s)) {
			jsonData = []byte(s)
		}
	} else if b, ok := data.([]byte); ok {
		if json.Valid(b) {
			jsonData = b
		}
	} else {
		var err error
		jsonData, err = json.Marshal(data)
		assert.NoError(t, err, "Failed to marshal data into JSON")
	}

	// Set the request body to the JSON
	req.Body = io.NopCloser(bytes.NewReader(jsonData))
	req.ContentLength = int64(len(jsonData))

	// Set the Content-Type header to application/json
	req.Header.Set("Content-Type", "application/json")
}

func newDb() context.Context {
	ctx := log.Logger.WithContext(context.Background())
	ctx = db.ConnCtx(ctx)
	return ctx
}
