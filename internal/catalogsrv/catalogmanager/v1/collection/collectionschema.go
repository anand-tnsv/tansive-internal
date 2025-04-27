package collection

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
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager/datatyperegistry"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/parameter"
	"github.com/tansive/tansive-internal/pkg/types"
)

type CollectionSchema struct {
	Version string                    `json:"version" validate:"required"`
	Spec    CollectionSpec            `json:"spec,omitempty"` // we can have empty collections
	Values  schemamanager.ParamValues `json:"-"`
}

type CollectionSpec struct {
	Parameters map[string]Parameter `json:"parameters,omitempty" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"`
	//Collections map[string]Collection `json:"collections" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"` // We don't maintain collection hierarcy here
}

type Parameter struct {
	Schema      string                    `json:"schema" validate:"required_without=DataType,omitempty,nameFormatValidator"`
	DataType    string                    `json:"dataType" validate:"required_without=Schema,excluded_unless=Schema '',omitempty,nameFormatValidator"`
	Default     types.NullableAny         `json:"default"`
	Annotations schemamanager.Annotations `json:"annotations" validate:"omitempty,dive,keys,noSpaces,endkeys"`
}

type Collection struct {
	Schema string `json:"schema" validate:"required,nameFormatValidator"`
}

func (cs *CollectionSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	// Note: We don't validate the dataType and default fields here
	// TODO: Add validation for dataType and default fields
	err := schemavalidator.V().Struct(cs)
	if err == nil {
		return nil
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
		case "required_without":
			ves = append(ves, schemaerr.ErrMissingSchemaOrType(jsonFieldName))
		case "excluded_unless":
			ves = append(ves, schemaerr.ErrShouldContainSchemaOrType(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "noSpaces":
			ves = append(ves, schemaerr.ErrInvalidAnnotation(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func (cs *CollectionSchema) ValidateDependencies(ctx context.Context, loaders schemamanager.SchemaLoaders, existingRefs schemamanager.SchemaReferences) (schemamanager.SchemaReferences, schemaerr.ValidationErrors) {
	var ves schemaerr.ValidationErrors
	var refs schemamanager.SchemaReferences
	var dataType schemamanager.ParamDataType
	refMap := make(map[string]schemamanager.SchemaReference)
	if loaders.ClosestParent == nil || loaders.ByHash == nil || loaders.ByPath == nil {
		return nil, append(ves, schemaerr.ErrMissingObjectLoaders(""))
	}

	if cs.Values == nil {
		cs.Values = make(schemamanager.ParamValues)
	}

	for n, p := range cs.Spec.Parameters {
		if p.Schema != "" {
			var schemaPath string
			for _, ref := range existingRefs {
				if ref.SchemaName() == p.Schema {
					schemaPath = ref.Name
					break
				}
			}
			var ref schemamanager.SchemaReference
			var ve schemaerr.ValidationErrors
			dataType, ref, ve = validateParameterSchemaDependency(ctx, loaders, n, schemaPath, &p)
			if ve != nil {
				ves = append(ves, ve...)
			} else {
				refMap[ref.Name] = ref
			}
		} else if p.DataType != "" {
			ves = append(ves, validateDataTypeDependency(n, &p, cs.Version)...)
			dataType = schemamanager.ParamDataType{
				Type:    p.DataType,
				Version: cs.Version,
			}
		}
		if dataType.Type != "" {
			cs.Values[n] = schemamanager.ParamValue{
				DataType:    dataType,
				Annotations: p.Annotations,
			}
		}
		cs.Spec.Parameters[n] = p
	}
	for _, ref := range refMap {
		refs = append(refs, ref)
	}
	return refs, ves
}

func (cs *CollectionSchema) ValidateValue(ctx context.Context, loaders schemamanager.SchemaLoaders, param string, value types.NullableAny) schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	if value.IsNil() {
		return ves
	}
	if loaders.ClosestParent == nil || loaders.ByPath == nil || loaders.ParameterRef == nil {
		return append(ves, schemaerr.ErrMissingObjectLoaders(""))
	}
	p, ok := cs.Spec.Parameters[param]
	if !ok {
		return append(ves, schemaerr.ErrInvalidParameter(param))
	}
	// shallow copy the parameter
	pShallowCopy := p
	pShallowCopy.Default = value
	if p.Schema != "" {
		schemaPath := loaders.ParameterRef(p.Schema)
		if schemaPath == "" {
			ves = append(ves, schemaerr.ErrInvalidParameter(param))
		} else {
			_, _, ve := validateParameterSchemaDependency(ctx, loaders, param, schemaPath, &pShallowCopy)
			ves = append(ves, ve...)
		}
	} else if p.DataType != "" {
		ves = append(ves, validateDataTypeDependency(param, &pShallowCopy, cs.Version)...)
	}
	return ves
}

func (cs *CollectionSchema) GetValue(ctx context.Context, param string) schemamanager.ParamValue {
	if cs.Values == nil {
		return schemamanager.ParamValue{}
	}
	p, ok := cs.Values[param]
	if !ok {
		return schemamanager.ParamValue{}
	}
	return p
}

func (cs *CollectionSchema) SetValue(ctx context.Context, param string, value types.NullableAny) error {
	if cs.Values == nil {
		return schemaerr.ErrInvalidParameter(param)
	}
	p, ok := cs.Values[param]
	if !ok {
		return schemaerr.ErrInvalidParameter(param)
	}
	p.Value = value
	cs.Values[param] = p
	return nil
}

func (cs *CollectionSchema) SetDefaultValues(ctx context.Context) {
	for n, p := range cs.Spec.Parameters {
		if p.Default.IsNil() {
			continue
		}
		cs.SetValue(ctx, n, p.Default)
	}
}

func (cs *CollectionSchema) ParameterNames() []string {
	var names []string
	for n := range cs.Spec.Parameters {
		names = append(names, n)
	}
	return names
}

func (cs *CollectionSchema) ParametersWithSchema(schemaName string) []schemamanager.ParameterSpec {
	var params []schemamanager.ParameterSpec
	for n, p := range cs.Spec.Parameters {
		if p.Schema == schemaName {
			ps := schemamanager.ParameterSpec{
				Name:    n,
				Default: p.Default,
			}
			if cs.Values != nil {
				if v, ok := cs.Values[n]; ok {
					ps.Value = v.Value
				}
			}
			params = append(params, ps)
		}
	}
	return params
}

func validateParameterSchemaDependency(ctx context.Context, loaders schemamanager.SchemaLoaders, name string, schemaPath string, p *Parameter) (schemamanager.ParamDataType, schemamanager.SchemaReference, schemaerr.ValidationErrors) {
	var ves schemaerr.ValidationErrors
	var ref schemamanager.SchemaReference
	var hash string
	var err apperrors.Error
	var dataType schemamanager.ParamDataType

	// find if there is an applicable parameter schema
	if schemaPath == "" {
		schemaPath, hash, err = loaders.ClosestParent(ctx, types.CatalogObjectTypeParameterSchema, p.Schema)
	}

	if err != nil || (schemaPath == "" && hash == "") {
		ves = append(ves, schemaerr.ErrParameterSchemaDoesNotExist(p.Schema))
	} else {
		ref = schemamanager.SchemaReference{
			Name: schemaPath,
		}

		var om schemamanager.SchemaManager
		var err apperrors.Error
		// construct the object metadata for what we want to load from the self metadata
		m := loaders.SelfMetadata()
		m.Name = path.Base(schemaPath)
		m.Path = path.Dir(schemaPath)
		if len(hash) > 0 {
			om, err = loaders.ByHash(ctx, types.CatalogObjectTypeParameterSchema, hash, &m)
		} else {
			om, err = loaders.ByPath(ctx, types.CatalogObjectTypeParameterSchema, &m)
		}
		if err != nil && om == nil {
			ves = append(ves, schemaerr.ErrParameterSchemaDoesNotExist(p.Schema))
			return dataType, ref, ves
		}
		pm := om.ParameterSchemaManager()
		if pm == nil {
			ves = append(ves, schemaerr.ErrParameterSchemaDoesNotExist(p.Schema))
			return dataType, ref, ves
		}
		dataType = pm.DataType()
		if !p.Default.IsNil() {
			if err := pm.ValidateValue(p.Default); err != nil {
				ves = append(ves, schemaerr.ErrInvalidValue(name, err.Error()))
			}
		} else {
			if pm.Default() != nil {
				p.Default, _ = types.NullableAnyFrom(pm.Default())
			}
		}
	}
	return dataType, ref, ves
}

func validateDataTypeDependency(name string, p *Parameter, version string) schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors

	//check if the DataType is supported
	loader := datatyperegistry.GetLoader(schemamanager.ParamDataType{
		Type:    p.DataType,
		Version: version,
	})
	if loader == nil {
		ves = append(ves, schemaerr.ErrUnsupportedDataType("spec.parameters.dataType", p.DataType))
		return ves
	}
	if !p.Default.IsNil() {
		appendError := func() {
			ves = append(ves, schemaerr.ErrInvalidValue(name))
		}
		// construct a spec
		dataTypeSpec := parameter.ParameterSpec{
			DataType: p.DataType,
			Default:  p.Default,
		}
		js, err := json.Marshal(dataTypeSpec)
		if err != nil {
			appendError()
			return ves
		}
		parameter, apperr := loader(js)
		if apperr != nil {
			appendError()
			return ves
		}
		if err := parameter.ValidateValue(p.Default); err != nil {
			ves = append(ves, schemaerr.ErrInvalidValue(name, err.Error()))
			return ves
		}
	}
	return ves
}
