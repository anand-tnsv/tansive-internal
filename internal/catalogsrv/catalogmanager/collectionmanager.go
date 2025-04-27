package catalogmanager

import (
	"context"
	"encoding/json"
	"path"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/errors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
)

type collectionSchema struct {
	Version    string                       `json:"version" validate:"required"`
	Kind       string                       `json:"kind" validate:"required,oneof=Collection"`
	Metadata   schemamanager.SchemaMetadata `json:"metadata" validate:"required"`
	Spec       collectionSpec               `json:"spec" validate:"required"`
	Values     schemamanager.ParamValues    `json:"-"`
	SchemaPath string                       `json:"-"`
}

type collectionSpec struct {
	Schema string                       `json:"schema" validate:"required,nameFormatValidator"`
	Values map[string]types.NullableAny `json:"values"`
}

func (cs *collectionSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	if cs.Kind != types.CollectionKind {
		ves = append(ves, schemaerr.ErrUnsupportedKind("kind"))
	}
	err := schemavalidator.V().Struct(cs)
	if err == nil {
		return ves
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(cs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "oneof":
			ves = append(ves, schemaerr.ErrInvalidFieldSchema(jsonFieldName, e.Value().(string)))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "catalogVersionValidator":
			ves = append(ves, schemaerr.ErrInvalidCatalogVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

type collectionManager struct {
	schema collectionSchema                      // schema for the collection
	csm    schemamanager.CollectionSchemaManager // collection schema manager
}

func (cm *collectionManager) Schema() string {
	return cm.schema.Spec.Schema
}

func (cm *collectionManager) CollectionSchema() []byte {
	b, _ := json.Marshal(cm.schema.Spec)
	return b
}

func (cm *collectionManager) Metadata() schemamanager.SchemaMetadata {
	return cm.schema.Metadata
}

func (cm *collectionManager) FullyQualifiedName() string {
	m := cm.schema.Metadata
	return path.Clean(m.Path + "/" + m.Name)
}

func (cm *collectionManager) CollectionSchemaManager() schemamanager.CollectionSchemaManager {
	return cm.csm
}

func (cm *collectionManager) SetCollectionSchemaPath(path string) {
	cm.schema.SchemaPath = path
}

func (cm *collectionManager) GetCollectionSchemaPath() string {
	return cm.schema.SchemaPath
}

func (cm *collectionManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: cm.schema.Version,
		Type:    types.CatalogObjectTypeCatalogCollection,
	}
	s.Values, _ = json.Marshal(cm.schema.Values)
	s.Schema, _ = json.Marshal(cm.schema.Spec)
	s.Description = cm.schema.Metadata.Description
	s.Entropy = cm.schema.Metadata.GetEntropyBytes(types.CatalogObjectTypeCatalogCollection)
	return &s
}

func (cm *collectionManager) SetCollectionSchemaManager(csm schemamanager.CollectionSchemaManager) {
	cm.csm = csm
}

func (cm *collectionManager) Values() schemamanager.ParamValues {
	return cm.schema.Values
}

func (cm *collectionManager) SetDefaultValues(param ...string) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}
	// set default values for the collection as defined in the schema
	values := cm.csm.GetDefaultValues()
	if len(param) > 0 {
		if v, ok := values[param[0]]; ok {
			cm.schema.Values[param[0]] = v
		} else {
			return ErrInvalidParameter.Msg("invalid parameter: " + param[0])
		}
	} else {
		cm.schema.Values = values
	}
	return nil
}

func (cm *collectionManager) GetValue(ctx context.Context, param string) (types.NullableAny, apperrors.Error) {
	if v, ok := cm.schema.Values[param]; ok {
		return v.Value, nil
	}
	return types.NilAny(), ErrInvalidParameter.Msg("invalid parameter: " + param)
}

func (cm *collectionManager) GetValueJSON(ctx context.Context, param string) ([]byte, apperrors.Error) {
	if v, ok := cm.schema.Values[param]; ok {
		if j, err := json.Marshal(v); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
			return nil, errors.ErrUnableToLoadObject
		} else {
			return j, nil
		}
	}
	return nil, ErrInvalidParameter.Msg("invalid parameter: " + param)
}

func (cm *collectionManager) GetAllValuesJSON(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(cm.schema.Values)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
		return nil, errors.ErrUnableToLoadObject
	}
	return j, nil
}

func (cm *collectionManager) SetValue(ctx context.Context, schemaLoaders schemamanager.SchemaLoaders, param string, value types.NullableAny) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}
	if err := cm.csm.ValidateValue(ctx, schemaLoaders, param, value); err != nil {
		return err
	}
	// We need to copy the dataType and other annotations from the schema before we can copy over the value
	if cm.schema.Values == nil {
		cm.schema.Values = make(schemamanager.ParamValues)
	}
	v := cm.csm.GetValue(ctx, param)
	v.Value = value
	cm.schema.Values[param] = v
	return nil
}

func (cm *collectionManager) ValidateValues(ctx context.Context, schemaLoaders schemamanager.SchemaLoaders, currentValues schemamanager.ParamValues) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}

	// There are few things to unwrap here:
	// At this time, the schema has all the parameters set in its Values. And these values either have the default set or are nil. But
	// the dataTypes and other annotations are always set.  So we need to copy all these over to the collection and substitute with new
	// values if the collection had any new values defined. Or we will copy over the defaults. If no defaults are set, the param will be a NullableAny
	// with dataType and other annotations set.
	if cm.schema.Values == nil {
		cm.schema.Values = make(schemamanager.ParamValues)
	}
	for _, param := range cm.csm.ParameterNames() {
		var currentValue schemamanager.ParamValue
		if v, ok := currentValues[param]; ok {
			currentValue = v
		}
		if v, ok := cm.schema.Spec.Values[param]; ok {
			if currentValue.Value.Equals(v) {
				cm.schema.Values[param] = currentValue
				continue
			}
			// if the user set any new value, we'll validate it and set it. If validation fails, we will return an error.
			if err := cm.SetValue(ctx, schemaLoaders, param, v); err != nil {
				return err
			}
		} else if !currentValue.Value.IsNil() {
			cm.schema.Values[param] = currentValue
		} else {
			// the values in the schema are already either the default or nil. But the dataType and other annotations are set. So it is safe to just copy over.
			cm.schema.Values[param] = cm.csm.GetValue(ctx, param)
		}
	}
	return nil
}

func (cm *collectionManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(cm.schema)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
		return j, errors.ErrUnableToLoadObject
	}
	return j, nil
}
