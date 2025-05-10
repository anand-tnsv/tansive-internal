package apis

import (
	"errors"
	"io"
	"net/http"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/pkg/types"
)

// createObject creates a new resource object
func createObject(req *http.Request) (*httpx.Response, error) {
	ctx := req.Context()

	if req.Body == nil {
		return nil, httpx.ErrInvalidRequest("request body is required")
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	name, err := getResourceName(req)
	if err != nil {
		return nil, err
	}

	kind := getResourceKind(req)
	if kind == types.InvalidKind {
		return nil, httpx.ErrInvalidRequest("invalid resource kind")
	}

	if err := validateRequest(body, kind); err != nil {
		return nil, err
	}

	manager, err := catalogmanager.ResourceManagerForKind(ctx, kind, name)
	if err != nil {
		return nil, err
	}

	resourceLoc, err := manager.Create(ctx, body)
	if err != nil {
		if errors.Is(err, catalogmanager.ErrInvalidVariant) {
			return nil, httpx.ErrInvalidVariant()
		}
		return nil, err
	}

	resp := &httpx.Response{
		StatusCode: http.StatusCreated,
		Location:   resourceLoc,
		Response:   nil,
	}

	return resp, nil
}
