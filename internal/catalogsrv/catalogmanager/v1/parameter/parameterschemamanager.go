package parameter

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager/datatyperegistry"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
)

type V1ParameterSchemaManager struct {
	version         string
	parameterSchema ParameterSchema
	parameter       schemamanager.Parameter
}

var _ schemamanager.ParameterSchemaManager = &V1ParameterSchemaManager{} // Ensure V1ParameterSchemaManager implements schemamanager.ParameterSchemaManager

func NewV1ParameterSchemaManager(ctx context.Context, version string, rsrcJson []byte, options ...schemamanager.Options) (*V1ParameterSchemaManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	// Read the parameter schema
	ps := &ParameterSchema{}
	err := json.Unmarshal(rsrcJson, ps)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter schema")
	}
	if o.Validate {
		ves := ps.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	// load the parameter spec
	loader := datatyperegistry.GetLoader(schemamanager.ParamDataType{
		Type:    ps.Spec.DataType,
		Version: version,
	})

	if loader == nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(schemaerr.ErrUnsupportedDataType("spec.dataType", ps.Spec.DataType).Error())
	}

	js, err := json.Marshal(ps.Spec)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter spec")
	}
	parameter, apperr := loader(js)
	if apperr != nil {
		return nil, apperr
	}
	if o.Validate {
		ves := parameter.ValidateSpec()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	return &V1ParameterSchemaManager{
		version:         version,
		parameterSchema: *ps,
		parameter:       parameter,
	}, nil
}

func (pm *V1ParameterSchemaManager) DataType() schemamanager.ParamDataType {
	return schemamanager.ParamDataType{
		Type:    pm.parameterSchema.Spec.DataType,
		Version: pm.version,
	}
}

func (pm *V1ParameterSchemaManager) Default() interface{} {
	return pm.parameter.DefaultValue()
}

func (pm *V1ParameterSchemaManager) ValidateValue(value types.NullableAny) apperrors.Error {
	return pm.parameter.ValidateValue(value)
}

func (pm *V1ParameterSchemaManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: pm.version,
		Type:    types.CatalogObjectTypeParameterSchema,
	}
	s.Schema, _ = json.Marshal(pm.parameterSchema.Spec)
	return &s
}

func (pm *V1ParameterSchemaManager) ValidateDependencies(ctx context.Context, loaders schemamanager.SchemaLoaders, collectionRefs schemamanager.SchemaReferences) (err apperrors.Error) {
	var ves schemaerr.ValidationErrors
	defer func() {
		if ves != nil {
			err = validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}()
	if loaders.ByHash == nil {
		return
	}
	for _, collectionRef := range collectionRefs {
		m := loaders.SelfMetadata()
		m.Path = collectionRef.Path()
		m.Name = collectionRef.SchemaName()
		om, err := loaders.ByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m)
		if err != nil {
			log.Ctx(ctx).Error().Str("collectionschema", collectionRef.Name).Msg("failed to load collection")
			continue
		}
		cm := om.CollectionSchemaManager()
		if cm == nil {
			log.Ctx(ctx).Error().Str("collectionschema", collectionRef.Name).Msg("failed to load collection manager")
			continue
		}
		p := cm.ParametersWithSchema(loaders.SelfMetadata().Name)
		for _, param := range p {
			attrib := collectionRef.Name + "/" + param.Name
			if !param.Default.IsNil() {
				if err := pm.ValidateValue(param.Default); err != nil {
					ves = append(ves, schemaerr.ErrInvalidValue(attrib+"::default", err.Error()))
				}
			}
		}
	}
	return
}
