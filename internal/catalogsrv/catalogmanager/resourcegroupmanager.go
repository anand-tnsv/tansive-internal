package catalogmanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"reflect"
	"slices"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	json "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/objectstore"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
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

func (r *Resource) JSON(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(r)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
		return j, ErrUnableToLoadObject
	}
	return j, nil
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

func (rgm *resourceGroupManager) StorageRepresentation() *objectstore.ObjectStorageRepresentation {
	s := objectstore.ObjectStorageRepresentation{
		Version: rgm.resourceGroup.Version,
		Type:    types.CatalogObjectTypeResourceGroup,
	}
	s.Values, _ = json.Marshal(rgm.resourceGroup.Spec)
	s.Spec, _ = json.Marshal(rgm.resourceGroup.Spec)
	s.Description = rgm.resourceGroup.Metadata.Description
	s.Entropy = rgm.resourceGroup.Metadata.GetEntropyBytes(types.CatalogObjectTypeResourceGroup)
	return &s
}

func (rgm *resourceGroupManager) GetStoragePath() string {
	m := rgm.Metadata()
	return getResourceGroupStoragePath(&m)
}

func getResourceGroupStoragePath(m *schemamanager.SchemaMetadata) string {
	t := types.CatalogObjectTypeResourceGroup
	rsrcPath := m.GetStoragePath(t)
	pathWithName := path.Clean(rsrcPath + "/" + m.Name)
	return pathWithName
}

// Save saves the resource group to the database.
// It handles the creation or update of both the resource group and its associated catalog object.
func (rgm *resourceGroupManager) Save(ctx context.Context) apperrors.Error {
	if rgm == nil {
		return validationerrors.ErrEmptySchema
	}

	t := types.CatalogObjectTypeResourceGroup

	m := rgm.Metadata()
	s := rgm.StorageRepresentation()
	storagePath := rgm.GetStoragePath()

	data, err := s.Serialize()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to serialize resource group")
		return err
	}
	newHash := s.GetHash()

	// Store this object and update the reference
	obj := models.CatalogObject{
		Type:    t,
		Hash:    newHash,
		Version: s.Version,
		Data:    data,
	}
	rgModel := models.ResourceGroup{
		Path:      storagePath,
		Hash:      newHash,
		VariantID: common.GetVariantIdFromContext(ctx),
	}

	// Get the directory ID for the resource group
	catalogID := common.GetCatalogIdFromContext(ctx)
	if catalogID == uuid.Nil {
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, m.Catalog)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("catalog", m.Catalog).Msg("Failed to get catalog ID by name")
			return err
		}
	}

	v, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, m.Variant.String())
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("catalogID", catalogID.String()).Str("name", m.Name).Msg("Failed to get variant")
		return err
	}

	err = db.DB(ctx).UpsertResourceGroupObject(ctx, &rgModel, &obj, v.ResourceGroupsDirectoryID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", storagePath).Msg("Failed to upsert resource group object")
		return err
	}

	return nil
}

func DeleteResourceGroup(ctx context.Context, m *schemamanager.SchemaMetadata) apperrors.Error {
	if m == nil {
		return ErrEmptyMetadata
	}

	storagePath := getResourceGroupStoragePath(m)
	// Get the directory ID for the resource group
	catalogID := common.GetCatalogIdFromContext(ctx)
	var err apperrors.Error
	if catalogID == uuid.Nil {
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, m.Catalog)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("catalog", m.Catalog).Msg("Failed to get catalog ID by name")
			return err
		}
	}

	v, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, m.Variant.String())
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("catalogID", catalogID.String()).Str("name", m.Name).Msg("Failed to get variant")
		return err
	}

	hash, err := db.DB(ctx).DeleteResourceGroup(ctx, storagePath, v.ResourceGroupsDirectoryID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", storagePath).Msg("Failed to delete resource group object")
		return err
	}

	if hash != "" {
		err = db.DB(ctx).DeleteCatalogObject(ctx, types.CatalogObjectTypeResourceGroup, hash)
		if !errors.Is(err, dberror.ErrNotFound) {
			// we don't return an error since the object reference has already been removed and
			// we cannot roll this back.
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete object from database")
		}
	} else {
		log.Ctx(ctx).Warn().Str("path", storagePath).Msg("resource group object not found")
		return ErrResourceGroupNotFound
	}

	return nil
}

func (rgm *resourceGroupManager) JSON(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(rgm.resourceGroup)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
		return j, ErrUnableToLoadObject
	}
	return j, nil
}
