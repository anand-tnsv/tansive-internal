package string

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager/datatyperegistry"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

const (
	dataType = "String"
	version  = "v1"
)

type Spec struct {
	DataType string            `json:"dataType" validate:"required,eq=String"`
	Value    types.NullableAny `json:"value,omitempty" validate:"omitnil"`
}

var _ schemamanager.DataType = &Spec{}
var _ datatyperegistry.Loader = LoadStringSpec // Ensure LoadStringSpec is a valid Loader

func (ss *Spec) ValidateSpec() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(ss)
	if err == nil {
		return nil
	}

	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		ves = append(ves, schemaerr.ErrInvalidFieldSchema(""))
		return ves
	}

	value := reflect.ValueOf(ss).Elem()
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

func (ss *Spec) GetValue() any {
	if !ss.Value.IsNil() {
		var v string
		if err := ss.Value.GetAs(&v); err != nil {
			return nil
		}
		return v
	}
	return nil
}

func (ss *Spec) ValidateValue(v types.NullableAny) apperrors.Error {
	var val string
	if err := v.GetAs(&val); err != nil {
		return validationerrors.ErrInvalidDataType.Msg("invalid string value")
	}
	return nil
}

func (ss *Spec) GetMIMEType() string {
	return "application/json"
}

func LoadStringSpec(data []byte) (schemamanager.DataType, apperrors.Error) {
	ss := &Spec{}
	err := json.Unmarshal(data, ss)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read string")
	}
	var v string
	if err := ss.Value.GetAs(&v); err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read string")
	}
	return ss, nil
}

func init() {
	datatyperegistry.RegisterDataType(schemamanager.ParamDataType{
		Type:    dataType,
		Version: version,
	}, LoadStringSpec)
}
