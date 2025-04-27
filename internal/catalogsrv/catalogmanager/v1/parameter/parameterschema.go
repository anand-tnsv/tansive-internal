package parameter

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/datatypes"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ParameterSchema struct {
	Spec ParameterSpec `json:"spec,omitempty" validate:"required"`
}

type ParameterSpec struct {
	DataType   string            `json:"dataType" validate:"required"`
	Validation json.RawMessage   `json:"validation"`
	Default    types.NullableAny `json:"default"`
}

func (ps *ParameterSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(ps)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(ps).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func ReadParameterSchema(version string, metadata, spec []byte) (*ParameterSchema, apperrors.Error) {
	ps := ParameterSchema{}
	err := json.Unmarshal(spec, &ps.Spec)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter spec")
	}
	return &ps, nil
}
