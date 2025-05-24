package catalogmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"reflect"
	"slices"

	"github.com/go-playground/validator/v10"
	"github.com/santhosh-tekuri/jsonschema/v5"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/gjson"
)

type ResourceGroup struct {
	Version  string                       `json:"version" validate:"required,requireVersionV1"`
	Kind     string                       `json:"kind" validate:"required,oneof=ResourceGroup"`
	Metadata schemamanager.SchemaMetadata `json:"metadata" validate:"required"`
	Spec     ResourceGroupSpec            `json:"spec,omitempty"` // we can have empty collections
}

type ResourceGroupSpec struct {
	Resources map[string]Resource `json:"resources,omitempty" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"`
}

type Resource struct {
	Provider    ResourceProvider          `json:"-" validate:"required_without=Schema,omitempty,nameFormatValidator"`
	Schema      json.RawMessage           `json:"schema" validate:"required_without=Provider,omitempty"`
	Value       types.NullableAny         `json:"value" validate:"omitempty"`
	Policy      string                    `json:"policy" validate:"omitempty,oneof=inherit override"`
	Annotations schemamanager.Annotations `json:"annotations" validate:"omitempty,dive,keys,noSpaces,endkeys"`
}

// ResourceProvider is a placeholder for the resource provider.
type ResourceProvider struct {
	_ any `json:"-"`
}

func (rg *ResourceGroup) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	if rg.Kind != types.ResourceGroupKind {
		validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind("kind"))
	}

	err := schemavalidator.V().Struct(rg)
	if err == nil {
		// Validate the schema if it is a valid json schema by using Santhosh Tekuri library
		for name, resource := range rg.Spec.Resources {
			if len(resource.Schema) > 0 {
				var compiledSchema *jsonschema.Schema
				compiledSchema, err = compileSchema(string(resource.Schema))
				if err != nil {
					validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(fmt.Sprintf("resource %s: %v", name, err)))
				}
				if compiledSchema != nil {
					// validate the value against the schema
					if err := resource.ValidateValue(resource.Value, compiledSchema); err != nil {
						validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(fmt.Sprintf("resource %s: %v", name, err)))
					}
				}
			}
		}
		return validationErrors
	}

	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(validationErrors, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(rg).Elem()
	typeOfCS := value.Type()

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "oneof":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidFieldSchema(jsonFieldName, e.Value().(string)))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		default:
			val := e.Value()
			param := e.Param()
			s := fmt.Sprintf("%v: %v", param, val)
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(s))
		}
	}
	return validationErrors
}

// ValidateValue validates a value against the resource's JSON schema
func (rg *Resource) ValidateValue(value types.NullableAny, optsCompiledSchema ...*jsonschema.Schema) error {
	var compiledSchema *jsonschema.Schema
	var err error
	if len(optsCompiledSchema) == 0 {
		compiledSchema, err = compileSchema(string(rg.Schema))
		if err != nil {
			return fmt.Errorf("failed to compile schema: %w", err)
		}
	} else {
		compiledSchema = optsCompiledSchema[0]
	}
	if compiledSchema == nil {
		return fmt.Errorf("failed to compile schema")
	}

	// Handle nil values - only reject if schema doesn't allow null
	if value.IsNil() {
		// Check if schema allows null type
		if !slices.Contains(compiledSchema.Types, "null") {
			return fmt.Errorf("value cannot be nil")
		}
		return nil
	}

	return compiledSchema.Validate(value.Get())
}

func compileSchema(schema string) (*jsonschema.Schema, error) {
	// First validate that the schema is valid JSON using gjson
	if !gjson.Valid(schema) {
		return nil, fmt.Errorf("invalid JSON schema")
	}

	compiler := jsonschema.NewCompiler()
	// Allow schemas with $id to refer to themselves
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		if url == "inline://schema" {
			return io.NopCloser(bytes.NewReader([]byte(schema))), nil
		}
		return nil, fmt.Errorf("unsupported schema ref: %s", url)
	}
	err := compiler.AddResource("inline://schema", bytes.NewReader([]byte(schema)))
	if err != nil {
		return nil, fmt.Errorf("failed to add schema resource: %w", err)
	}
	compiledSchema, err := compiler.Compile("inline://schema")
	if err != nil {
		return nil, fmt.Errorf("failed to compile schema: %w", err)
	}

	return compiledSchema, nil
}

type resourceGroupManager struct {
	resourceGroup ResourceGroup
}

func (rgm *resourceGroupManager) Metadata() schemamanager.SchemaMetadata {
	return rgm.resourceGroup.Metadata
}

func (rgm *resourceGroupManager) FullyQualifiedName() string {
	m := rgm.resourceGroup.Metadata
	return path.Clean(m.Path + "/" + m.Name)
}

func (rgm *resourceGroupManager) SetValue(ctx context.Context, resourceName string, value types.NullableAny) apperrors.Error {
	resource, ok := rgm.resourceGroup.Spec.Resources[resourceName]
	if !ok {
		return ErrInvalidResourceValue.Msg(fmt.Sprintf("resource %s not found", resourceName))
	}
	// validate the value against the schema
	if err := resource.ValidateValue(value); err != nil {
		return ErrInvalidResourceValue.Msg(err.Error())
	}
	resource.Value = value
	rgm.resourceGroup.Spec.Resources[resourceName] = resource
	return nil
}

func (rgm *resourceGroupManager) GetValue(ctx context.Context, resourceName string) (types.NullableAny, apperrors.Error) {
	resource, ok := rgm.resourceGroup.Spec.Resources[resourceName]
	if !ok {
		return types.NilAny(), ErrInvalidResourceValue.Msg(fmt.Sprintf("resource %s not found", resourceName))
	}
	return resource.Value, nil
}

func (rgm *resourceGroupManager) GetValueJSON(ctx context.Context, resourceName string) ([]byte, apperrors.Error) {
	resource, ok := rgm.resourceGroup.Spec.Resources[resourceName]
	if !ok {
		return nil, ErrInvalidResourceValue.Msg(fmt.Sprintf("resource %s not found", resourceName))
	}
	json, err := json.Marshal(resource.Value)
	if err != nil {
		return nil, ErrInvalidResourceValue.Msg("unable to obtain resource value")
	}
	return json, nil
}
