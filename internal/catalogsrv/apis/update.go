package apis

import (
	"io"
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/pkg/types"
)

// Create a new resource object
func updateObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	n, err := getResourceName(r)
	if err != nil {
		return nil, err
	}
	kind = getResourceKind(r)
	if kind == types.InvalidKind {
		return nil, httpx.ErrInvalidRequest()
	}

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}
	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}
	if err := validateRequest(req, kind); err != nil {
		return nil, err
	}

	rm, err := catalogmanager.ResourceManagerForKind(ctx, kind, n)
	if err != nil {
		return nil, err
	}
	err = rm.Update(ctx, req)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   nil,
	}
	return rsp, nil
}
