package collection

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
)

type V1CollectionSchemaManager struct {
	version          string
	collectionSchema CollectionSchema
}

func NewV1CollectionSchemaManager(ctx context.Context, version string, rsrcJson []byte, options ...schemamanager.Options) (*V1CollectionSchemaManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	// Read the collection schema
	cs := &CollectionSchema{}
	err := json.Unmarshal(rsrcJson, cs)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read collection schema")
	}

	// Just to ensure we have consistent version throughout, let's update cs with the version
	cs.Version = version

	if o.Validate {
		ves := cs.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	if o.ValidateDependencies {
		_, ves := cs.ValidateDependencies(ctx, o.SchemaLoaders, schemamanager.SchemaReferences{})
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	if o.SetDefaultValues {
		cs.SetDefaultValues(ctx)
	}

	if o.ParamValues != nil {
		err := json.Unmarshal(o.ParamValues, &cs.Values)
		if err != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter values")
		}
	}

	return &V1CollectionSchemaManager{
		version:          version,
		collectionSchema: *cs,
	}, nil
}

func (cm *V1CollectionSchemaManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: cm.version,
		Type:    catcommon.CatalogObjectTypeCollectionSchema,
	}
	s.Values, _ = json.Marshal(cm.collectionSchema.Values)
	s.Schema, _ = json.Marshal(cm.collectionSchema.Spec)
	return &s
}

func (cm *V1CollectionSchemaManager) ParameterNames() []string {
	return cm.collectionSchema.ParameterNames()
}

func (cm *V1CollectionSchemaManager) ParametersWithSchema(schemaName string) []schemamanager.ParameterSpec {
	return cm.collectionSchema.ParametersWithSchema(schemaName)
}

func (cm *V1CollectionSchemaManager) ValidateDependencies(ctx context.Context, loaders schemamanager.SchemaLoaders, existingRefs schemamanager.SchemaReferences) (schemamanager.SchemaReferences, apperrors.Error) {
	refs, ves := cm.collectionSchema.ValidateDependencies(ctx, loaders, existingRefs)
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	return refs, nil
}

func (cm *V1CollectionSchemaManager) ValidateValue(ctx context.Context, loaders schemamanager.SchemaLoaders, param string, value types.NullableAny) apperrors.Error {
	ves := cm.collectionSchema.ValidateValue(ctx, loaders, param, value)
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	return nil
}

func (cm *V1CollectionSchemaManager) GetValue(ctx context.Context, param string) schemamanager.ParamValue {
	return cm.collectionSchema.GetValue(ctx, param)
}

func (cm *V1CollectionSchemaManager) GetDefaultValues() map[string]schemamanager.ParamValue {
	return cm.collectionSchema.Values
}

func (cm *V1CollectionSchemaManager) SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error {
	err := cm.collectionSchema.SetValue(ctx, param, value)
	if err != nil {
		return validationerrors.ErrSchemaValidation.Msg(err.Error())
	}
	return nil
}

func (cm *V1CollectionSchemaManager) SetDefaultValues(ctx context.Context) {
	cm.collectionSchema.SetDefaultValues(ctx)
}
