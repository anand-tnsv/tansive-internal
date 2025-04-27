package apis

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/pkg/types"
)

func deleteObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()

	n, err := getResourceName(r)
	if err != nil {
		return nil, err
	}
	kind := getResourceKind(r)
	if kind == types.InvalidKind {
		return nil, httpx.ErrInvalidRequest()
	}

	rm, err := catalogmanager.ResourceManagerForKind(ctx, kind, n)
	if err != nil {
		return nil, err
	}

	err = rm.Delete(ctx)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusNoContent,
		Response:   nil,
	}
	return rsp, nil
}
