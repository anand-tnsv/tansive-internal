package integer

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager/datatyperegistry"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

const (
	dataType = "Integer"
	version  = "v1"
)

type Validation struct {
	MinValue *int `json:"minValue" validate:"omitnil"`
	MaxValue *int `json:"maxValue" validate:"omitnil,integerBoundsValidator"`
	Step     *int `json:"step" validate:"omitnil,stepValidator"`
}

type Spec struct {
	DataType   string            `json:"dataType" validate:"required,eq=Integer"`
	Validation *Validation       `json:"validation,omitempty" validate:"omitnil"`
	Default    types.NullableAny `json:"default,omitempty" validate:"omitnil"`
}

var _ schemamanager.Parameter = &Spec{}         // Ensure Spec implements schemamanager.Parameter
var _ datatyperegistry.Loader = LoadIntegerSpec // Ensure LoadIntegerSpec is a valid Loader

func (is *Spec) ValidateSpec() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(is)
	if err == nil {
		// validate the default value
		if is.Validation != nil && !is.Default.IsNil() {
			err := is.ValidateValue(is.Default)
			if err != nil {
				return append(ves, schemaerr.ValidationError{
					Field:  "default",
					ErrStr: err.Error(),
				})
			}
		}
		return nil
	}

	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		ves = append(ves, schemaerr.ErrInvalidFieldSchema(""))
		return ves
	}

	value := reflect.ValueOf(is).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "stepValidator":
			ves = append(ves, schemaerr.ErrInvalidStepValue(jsonFieldName))
		case "integerBoundsValidator":
			ves = append(ves, schemaerr.ErrMaxValueLessThanMinValue(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	return ves
}

func (is *Spec) DefaultValue() any {
	if !is.Default.IsNil() {
		var v int
		if err := is.Default.GetAs(&v); err != nil {
			return nil
		}
		return v
	}
	return nil
}

func LoadIntegerSpec(data []byte) (schemamanager.Parameter, apperrors.Error) {
	is := &Spec{}
	err := json.Unmarshal(data, is)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read integer schema")
	}
	return is, nil
}

func init() {
	schemavalidator.V().RegisterValidation("stepValidator", integerStepValidator)
	schemavalidator.V().RegisterValidation("integerBoundsValidator", integerBoundsValidator)

	datatyperegistry.RegisterDataType(schemamanager.ParamDataType{
		Type:    dataType,
		Version: version,
	}, LoadIntegerSpec)
}
