package schemaresource

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

/*
type ObjectHeader struct {
	Version string `json:"version" validate:"required"`
	Kind    string `json:"kind" validate:"required,kindValidator"`
}
*/
// SchemaResource represents a schema for a resource.
type SchemaResource struct {
	Version  string                       `json:"version" validate:"required"`
	Kind     string                       `json:"kind" validate:"required,kindValidator"`
	Metadata schemamanager.SchemaMetadata `json:"metadata" validate:"required"`
	Spec     json.RawMessage              `json:"spec,omitempty"`
}

func (rs *SchemaResource) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	if rs.Kind != types.CollectionSchemaKind && rs.Kind != types.ParameterSchemaKind {
		ves = append(ves, schemaerr.ErrUnsupportedKind("kind"))
	}
	err := schemavalidator.V().Struct(rs)
	if err == nil {
		return ves
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(rs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "kindValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrUnsupportedKind(jsonFieldName, val))
		case "resourceNameValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
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

// Read a json string in to a SchemaResource struct
func ReadSchemaResource(jsonStr string) (*SchemaResource, apperrors.Error) {
	rs := &SchemaResource{}
	err := json.Unmarshal([]byte(jsonStr), rs)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read resource schema")
	}
	return rs, nil
}

const SchemaResourceJsonSchema = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"version": {
				"type": "string"
			},
			"kind": {
				"type": "string",
			},
			"metadata": {
				"type": "object"
			},
			"spec": {
				"type": "object"
			}
		},
		"required": ["version", "kind", "metadata", "spec"],
		"additionalProperties": false
	}`
