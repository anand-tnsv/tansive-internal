package schemaresource

import (
	"context"
	"encoding/json"

	log "github.com/rs/zerolog/log"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/collection"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/parameter"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
)

type V1SchemaManager struct {
	resourceSchema          *SchemaResource
	parameterSchemaManager  *parameter.V1ParameterSchemaManager
	collectionSchemaManager *collection.V1CollectionSchemaManager
}

var _ schemamanager.SchemaManager = &V1SchemaManager{} // Ensure V1SchemaManager implements schemamanager.SchemaManager

func NewV1SchemaManager(ctx context.Context, rsrcJson []byte, options ...schemamanager.Options) (*V1SchemaManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	rs, err := ReadSchemaResource(string(rsrcJson))
	if err != nil {
		return nil, err
	}

	if rs.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}
	if o.Validate {
		ves := rs.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}
	return buildSchemaManager(ctx, rs, rsrcJson, options...)
}

func LoadV1SchemaManager(ctx context.Context, s *schemastore.SchemaStorageRepresentation, m *schemamanager.SchemaMetadata) (*V1SchemaManager, apperrors.Error) {
	rs := &SchemaResource{}
	rs.Version = s.Version
	switch s.Type {
	case catcommon.CatalogObjectTypeParameterSchema:
		rs.Kind = "ParameterSchema"
	case catcommon.CatalogObjectTypeCollectionSchema:
		rs.Kind = "CollectionSchema"
	}
	rs.Metadata = *m
	rs.Spec = s.Schema

	ves := rs.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	var opts []schemamanager.Options
	if len(s.Values) > 0 && json.Valid(s.Values) {
		opts = append(opts, schemamanager.WithParamValues(s.Values))
	}

	rs.Metadata.Description = s.Description
	return buildSchemaManager(ctx, rs, nil, opts...)
}

func buildSchemaManager(ctx context.Context, rs *SchemaResource, rsrcJson []byte, options ...schemamanager.Options) (*V1SchemaManager, apperrors.Error) {
	if rs == nil {
		return nil, validationerrors.ErrEmptySchema
	}
	if rsrcJson == nil {
		rsrcJson, _ = json.Marshal(rs)
	}
	rm := &V1SchemaManager{
		resourceSchema: rs,
	}

	// Initialize the appropriate manager based on the kind
	var err apperrors.Error
	switch rs.Kind {
	case "ParameterSchema":
		if rm.parameterSchemaManager, err = parameter.NewV1ParameterSchemaManager(ctx, rs.Version, rsrcJson, options...); err != nil {
			return nil, err
		}
	case "CollectionSchema":
		if rm.collectionSchemaManager, err = collection.NewV1CollectionSchemaManager(ctx, rs.Version, rsrcJson, options...); err != nil {
			return nil, err
		}
	default:
		return nil, validationerrors.ErrInvalidKind
	}

	return rm, nil

}

func (rm *V1SchemaManager) Version() string {
	return rm.resourceSchema.Version
}

func (rm *V1SchemaManager) Kind() string {
	return rm.resourceSchema.Kind
}

func (rm *V1SchemaManager) Type() catcommon.CatalogObjectType {
	switch rm.Kind() {
	case "ParameterSchema":
		return catcommon.CatalogObjectTypeParameterSchema
	case "CollectionSchema":
		return catcommon.CatalogObjectTypeCollectionSchema
	default:
		return catcommon.CatalogObjectTypeInvalid
	}
}
func (rm *V1SchemaManager) Metadata() schemamanager.SchemaMetadata {
	return rm.resourceSchema.Metadata
}

func (rm *V1SchemaManager) Name() string {
	return rm.resourceSchema.Metadata.Name
}

func (rm *V1SchemaManager) Path() string {
	return rm.resourceSchema.Metadata.Path
}

func (rm *V1SchemaManager) FullyQualifiedName() string {
	return rm.resourceSchema.Metadata.Path + "/" + rm.resourceSchema.Metadata.Name
}

func (rm *V1SchemaManager) Catalog() string {
	return rm.resourceSchema.Metadata.Catalog
}

func (rm *V1SchemaManager) Description() string {
	return rm.resourceSchema.Metadata.Description
}

func (rm *V1SchemaManager) SetName(name string) {
	rm.resourceSchema.Metadata.Name = name
}

func (rm *V1SchemaManager) SetPath(path string) {
	rm.resourceSchema.Metadata.Path = path
}

func (rm *V1SchemaManager) SetCatalog(catalog string) {
	rm.resourceSchema.Metadata.Catalog = catalog
}

func (rm *V1SchemaManager) SetDescription(description string) {
	rm.resourceSchema.Metadata.Description = description
}

func (rm *V1SchemaManager) SetMetadata(m *schemamanager.SchemaMetadata) {
	rm.resourceSchema.Metadata = *m
}

func (rm *V1SchemaManager) ParameterSchemaManager() schemamanager.ParameterSchemaManager {
	return rm.parameterSchemaManager
}

func (rm *V1SchemaManager) CollectionSchemaManager() schemamanager.CollectionSchemaManager {
	return rm.collectionSchemaManager
}

func (rm *V1SchemaManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	var s *schemastore.SchemaStorageRepresentation = nil
	switch rm.Kind() {
	case "ParameterSchema":
		if rm.parameterSchemaManager != nil {
			s = rm.parameterSchemaManager.StorageRepresentation()
		}
	case "CollectionSchema":
		if rm.collectionSchemaManager != nil {
			s = rm.collectionSchemaManager.StorageRepresentation()
		}
	}
	s.Description = rm.resourceSchema.Metadata.Description
	// We add entropy here because two schemas that have the same storage representation can be referred at multiple places
	s.Entropy = rm.resourceSchema.Metadata.GetEntropyBytes(rm.Type())
	return s
}

func (rm *V1SchemaManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(rm.resourceSchema)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
		return j, errors.ErrUnableToLoadObject
	}
	return j, nil
}

func (rm *V1SchemaManager) Compare(other schemamanager.SchemaManager, excludeMetadata bool) bool {
	thisObj := rm.StorageRepresentation()
	otherObj := other.StorageRepresentation()
	// to exclude metadata, just exclude description. If there are other values in future, we need to do more here.
	if excludeMetadata {
		thisObj.Description = ""
		otherObj.Description = ""
	}
	return thisObj.GetHash() == otherObj.GetHash()
}
