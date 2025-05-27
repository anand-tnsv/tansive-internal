package interfaces

import (
	"context"
	"net/url"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type KindHandler interface {
	Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error)
	Get(ctx context.Context) ([]byte, apperrors.Error)
	Delete(ctx context.Context) apperrors.Error
	Update(ctx context.Context, rsrcJson []byte) apperrors.Error
	List(ctx context.Context) ([]byte, apperrors.Error)
	Location() string
}

type RequestContext struct {
	Catalog        string
	CatalogID      uuid.UUID
	Variant        string
	VariantID      uuid.UUID
	Namespace      string
	ObjectName     string
	ObjectType     catcommon.CatalogObjectType
	ObjectPath     string
	ObjectProperty string
	QueryParams    url.Values
}

type KindHandlerFactory func(context.Context, RequestContext) (KindHandler, apperrors.Error)
