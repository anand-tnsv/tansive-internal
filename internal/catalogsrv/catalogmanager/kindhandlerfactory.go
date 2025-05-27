package catalogmanager

import (
	"context"
	"net/url"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tidwall/gjson"
)

type RequestContext struct {
	Catalog        string
	CatalogID      uuid.UUID
	Variant        string
	VariantID      uuid.UUID
	WorkspaceID    uuid.UUID
	WorkspaceLabel string
	Workspace      string
	Namespace      string
	ObjectName     string
	ObjectType     catcommon.CatalogObjectType
	ObjectPath     string
	ObjectProperty string
	QueryParams    url.Values
}

func RequestType(resourceJSON []byte) (kind string, err apperrors.Error) {
	if !gjson.ValidBytes(resourceJSON) {
		return "", ErrInvalidSchema.Msg("invalid JSON format")
	}

	result := gjson.GetBytes(resourceJSON, "kind")
	if !result.Exists() {
		return "", ErrInvalidSchema.Msg("missing kind field")
	}
	kind = result.String()

	result = gjson.GetBytes(resourceJSON, "version")
	if !result.Exists() {
		return "", ErrInvalidSchema.Msg("missing version field")
	}
	version := result.String()

	if schemavalidator.ValidateSchemaKind(kind) && version == catcommon.VersionV1 {
		return kind, nil
	}
	return "", ErrInvalidSchema.Msg("invalid kind or version")
}

type KindHandlerFactory func(context.Context, RequestContext) (interfaces.KindHandler, apperrors.Error)

var kindHandlerFactories = map[string]KindHandlerFactory{
	catcommon.CatalogKind:   NewCatalogKindHandler,
	catcommon.VariantKind:   NewVariantKindHandler,
	catcommon.NamespaceKind: NewNamespaceKindHandler,
	catcommon.ResourceKind:  NewResourceKindHandler,
	catcommon.ViewKind:      NewViewKindHandler,
}

func ResourceManagerForKind(ctx context.Context, kind string, name RequestContext) (interfaces.KindHandler, apperrors.Error) {
	factory, ok := kindHandlerFactories[kind]
	if !ok {
		return nil, ErrInvalidSchema.Msg("unsupported resource kind: " + kind)
	}
	return factory(ctx, name)
}
