package catalogmanager

import (
	"context"
	"net/url"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
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
	ObjectType     types.CatalogObjectType
	ObjectPath     string
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

	if schemavalidator.ValidateSchemaKind(kind) && version == types.VersionV1 {
		return kind, nil
	}
	return "", ErrInvalidSchema.Msg("invalid kind or version")
}

type ResourceManagerFactory func(context.Context, RequestContext) (schemamanager.ResourceManager, apperrors.Error)

var resourceFactories = map[string]ResourceManagerFactory{
	types.CatalogKind:          NewCatalogResource,
	types.VariantKind:          NewVariantResource,
	types.NamespaceKind:        NewNamespaceResource,
	types.WorkspaceKind:        NewWorkspaceResource,
	types.CollectionSchemaKind: NewSchemaResource,
	types.ParameterSchemaKind:  NewSchemaResource,
	types.CollectionKind:       NewCollectionResource,
	types.AttributeKind:        NewAttributeResource,
	types.ViewKind:             NewViewResource,
}

func ResourceManagerForKind(ctx context.Context, kind string, name RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	factory, ok := resourceFactories[kind]
	if !ok {
		return nil, ErrInvalidSchema.Msg("unsupported resource kind: " + kind)
	}
	return factory(ctx, name)
}
