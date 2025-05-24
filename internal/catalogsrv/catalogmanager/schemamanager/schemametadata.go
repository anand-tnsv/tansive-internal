package schemamanager

import (
	"encoding/json"
	"path"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/pkg/types"
)

// Modifying this struct should also change the json
type SchemaMetadata struct {
	Name        string               `json:"name" validate:"required,resourceNameValidator"`
	Catalog     string               `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     types.NullableString `json:"variant,omitempty" validate:"resourceNameValidator"`
	Namespace   types.NullableString `json:"namespace,omitempty" validate:"omitempty,resourceNameValidator"`
	Path        string               `json:"path,omitempty" validate:"omitempty,resourcePathValidator"`
	Description string               `json:"description"`
	IDS         IDS                  `json:"-"`
}

type IDS struct {
	CatalogID uuid.UUID
	VariantID uuid.UUID
}

var _ json.Marshaler = SchemaMetadata{}
var _ json.Marshaler = &SchemaMetadata{}

func (rs *SchemaMetadata) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(rs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(rs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		jsonFieldName = "metadata." + jsonFieldName
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "resourceNameValidator":
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

func (s SchemaMetadata) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})

	m["name"] = s.Name
	m["catalog"] = s.Catalog
	m["description"] = s.Description

	if s.Variant.Valid {
		m["variant"] = s.Variant.Value
	}
	if s.Namespace.Valid {
		m["namespace"] = s.Namespace.Value
	}
	if s.Path != "" {
		m["path"] = s.Path
	}

	return json.Marshal(m)
}

func (m SchemaMetadata) GetStoragePath(t types.CatalogObjectType) string {
	if t == types.CatalogObjectTypeCatalogCollection {
		if m.Namespace.IsNil() {
			return path.Clean("/" + types.DefaultNamespace + "/" + m.Path)
		} else {
			return path.Clean("/" + types.DefaultNamespace + "/" + m.Namespace.String() + "/" + m.Path)
		}
	} else {
		if m.Namespace.IsNil() {
			return "/" + types.DefaultNamespace
		} else {
			return "/" + types.DefaultNamespace + "/" + m.Namespace.String()
		}
	}
}

func (m SchemaMetadata) GetEntropyBytes(t types.CatalogObjectType) []byte {
	entropy := m.Catalog + ":" + string(t)
	return []byte(entropy)
}
