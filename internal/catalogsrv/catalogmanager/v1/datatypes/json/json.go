package json

import (
	"bytes"
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/santhosh-tekuri/jsonschema/v5"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager/datatyperegistry"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

const (
	dataType = "JSON"
	version  = "v1"
)

type Spec struct {
	DataType string            `json:"dataType" validate:"required,eq=JSON"`
	Value    types.NullableAny `json:"value,omitempty" validate:"omitnil"`
	Schema   string            `json:"schema" validate:"required"`
}

var _ schemamanager.DataType = &Spec{}
var _ datatyperegistry.Loader = LoadJSONSpec // Ensure LoadJSONSpec is a valid Loader

func (js *Spec) ValidateSpec() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(js)
	if err == nil {
		return nil
	}

	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		ves = append(ves, schemaerr.ErrInvalidFieldSchema(""))
		return ves
	}

	value := reflect.ValueOf(js).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	return ves
}

func (js *Spec) GetValue() any {
	return js.Value.Get()
}

func (js *Spec) ValidateValue(v types.NullableAny) apperrors.Error {
	if v.IsNil() {
		return nil
	}

	// Create schema compiler
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", bytes.NewReader([]byte(js.Schema))); err != nil {
		return validationerrors.ErrInvalidDataType.Msg("invalid JSON schema")
	}

	// Compile the schema
	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return validationerrors.ErrInvalidDataType.Msg("invalid JSON schema: " + err.Error())
	}

	// Validate the document against the schema
	if err := schema.Validate(v.Get()); err != nil {
		return validationerrors.ErrInvalidDataType.Msg("JSON validation failed: " + err.Error())
	}

	return nil
}

func (js *Spec) GetMIMEType() string {
	return "application/json"
}

func LoadJSONSpec(data []byte) (schemamanager.DataType, apperrors.Error) {
	js := &Spec{}
	err := json.Unmarshal(data, js)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read JSON")
	}

	// Create schema compiler
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", bytes.NewReader([]byte(js.Schema))); err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("invalid JSON schema")
	}

	// Try to compile the schema to validate it
	if _, err := compiler.Compile("schema.json"); err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("invalid JSON schema: " + err.Error())
	}

	// If there's a value, validate it against the schema
	if !js.Value.IsNil() {
		if err := js.ValidateValue(js.Value); err != nil {
			return nil, err
		}
	}

	return js, nil
}

func init() {
	datatyperegistry.RegisterDataType(schemamanager.ParamDataType{
		Type:    dataType,
		Version: version,
	}, LoadJSONSpec)
}
