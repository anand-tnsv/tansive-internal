package apis

import (
	"testing"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	err := ToHttpxError(nil)
	assert.Nil(t, err)
	appError := apperrors.New("test error").SetStatusCode(500)
	herr := ToHttpxError(appError)
	assert.NotNil(t, herr)
	assert.Equal(t, 500, herr.(*httpx.Error).StatusCode)
	assert.Equal(t, "test error", herr.(*httpx.Error).Description)
}
