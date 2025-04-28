package httpx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockRequester struct {
	ProjectId string `json:"project_id"`
	TenantId  string `json:"tenant_id"`
}

func (r *mockRequester) RequestMethod() (string, string) {
	return "GET", "/tenant/{tenant_id}/projects/{project_id}"
}

func TestResolvPath(t *testing.T) {
	req := mockRequester{
		TenantId:  "123",
		ProjectId: "123",
	}
	path, err := resolvePath(&req)
	assert.NoError(t, err)
	assert.Equal(t, "/tenant/123/projects/123", path)
}
