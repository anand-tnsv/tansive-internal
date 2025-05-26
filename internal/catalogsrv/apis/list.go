package apis

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/pkg/types"
)

func listObjects(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	reqContext, err := getRequestContext(r)
	if err != nil {
		return nil, err
	}

	kind = getResourceKind(r)
	if kind == types.InvalidKind {
		return nil, httpx.ErrInvalidRequest()
	}

	rm, err := catalogmanager.ResourceManagerForKind(ctx, kind, reqContext)
	if err != nil {
		return nil, err
	}

	rsrc, err := rm.List(ctx)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   rsrc,
	}
	return rsp, nil
}
