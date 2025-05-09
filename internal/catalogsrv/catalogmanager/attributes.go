package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"path"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/gjson"
)

// GetAttribute retrieves a single attribute value from a collection
func GetAttribute(ctx context.Context, metadata *schemamanager.SchemaMetadata, attrName string, opts ...ObjectStoreOption) ([]byte, apperrors.Error) {
	if metadata == nil || attrName == "" {
		return nil, validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	collection, err := loadCollectionObjectByPath(ctx, metadata, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Msg("collection not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
		return nil, err
	}

	cm, err := collectionManagerFromObject(ctx, collection, metadata)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection")
		return nil, err
	}

	return cm.GetValueJSON(ctx, attrName)
}

// GetAllAttributes retrieves all attribute values from a collection
func GetAllAttributes(ctx context.Context, metadata *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) ([]byte, apperrors.Error) {
	if metadata == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	collection, err := loadCollectionObjectByPath(ctx, metadata, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Msg("collection not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
		return nil, err
	}

	cm, err := collectionManagerFromObject(ctx, collection, metadata)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection")
		return nil, err
	}

	return cm.GetAllValuesJSON(ctx)
}

// DeleteAttribute removes an attribute from a collection
func DeleteAttribute(ctx context.Context, metadata *schemamanager.SchemaMetadata, attrName string, opts ...ObjectStoreOption) apperrors.Error {
	if metadata == nil || attrName == "" {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var dir Directories
	objType := types.CatalogObjectTypeCatalogCollection
	storagePath := path.Clean(metadata.GetStoragePath(objType) + "/" + metadata.Name)

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getWorkspaceDirs(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else if metadata.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, metadata.IDS.VariantID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	existingCollection, err := loadCollectionObjectByPath(ctx, metadata, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			existingCollection = nil
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return err
		}
	}

	var cm schemamanager.CollectionManager
	if existingCollection != nil {
		if options.ErrorIfExists {
			return ErrAlreadyExists.Msg("collection already exists")
		}
		cm, err = collectionManagerFromObject(ctx, existingCollection, metadata)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to load existing collection")
			return err
		}
	}

	schemaPath, _, err := setCollectionSchemaManager(ctx, cm, dir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set collection schema manager")
		return err
	}

	err = cm.SetDefaultValues(attrName)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set default values")
		return err
	}

	storage := cm.StorageRepresentation()
	data, err := storage.Serialize()
	if err != nil {
		return err
	}

	newHash := storage.GetHash()
	if existingCollection != nil && newHash == existingCollection.Hash {
		if options.ErrorIfEqualToExisting {
			return ErrEqualToExistingObject
		}
		return nil
	}

	obj := models.CatalogObject{
		Type:    types.CatalogObjectTypeCatalogCollection,
		Hash:    newHash,
		Version: storage.Version,
		Data:    data,
	}

	return saveCollectionObject(ctx, metadata, &obj, dir, storagePath, schemaPath)
}

// AttributeValues represents a map of attribute names to their values
type AttributeValues map[string]types.NullableAny

// UpdateAttributes updates multiple attributes in a collection
func UpdateAttributes(ctx context.Context, metadata *schemamanager.SchemaMetadata, values AttributeValues, opts ...ObjectStoreOption) apperrors.Error {
	if metadata == nil || values == nil {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var dir Directories
	objType := types.CatalogObjectTypeCatalogCollection
	storagePath := path.Clean(metadata.GetStoragePath(objType) + "/" + metadata.Name)

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getWorkspaceDirs(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else if metadata.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, metadata.IDS.VariantID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	existingCollection, err := loadCollectionObjectByPath(ctx, metadata, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			existingCollection = nil
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return err
		}
	}

	var cm schemamanager.CollectionManager
	if existingCollection != nil {
		if options.ErrorIfExists {
			return ErrAlreadyExists.Msg("collection already exists")
		}
		cm, err = collectionManagerFromObject(ctx, existingCollection, metadata)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to load existing collection")
			return err
		}
	}

	schemaPath, schemaLoaders, err := setCollectionSchemaManager(ctx, cm, dir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set collection schema manager")
		return err
	}

	for attrName, value := range values {
		existingValue, _ := cm.GetValue(ctx, attrName)
		if existingValue.Equals(value) {
			log.Ctx(ctx).Info().Msg("value unchanged, skipping update")
			continue
		}

		err = cm.SetValue(ctx, schemaLoaders, attrName, value)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to set value in collection manager")
			return err
		}
	}

	storage := cm.StorageRepresentation()
	data, err := storage.Serialize()
	if err != nil {
		return err
	}

	newHash := storage.GetHash()
	if existingCollection != nil && newHash == existingCollection.Hash {
		if options.ErrorIfEqualToExisting {
			return ErrEqualToExistingObject
		}
		return nil
	}

	obj := models.CatalogObject{
		Type:    types.CatalogObjectTypeCatalogCollection,
		Hash:    newHash,
		Version: storage.Version,
		Data:    data,
	}

	return saveCollectionObject(ctx, metadata, &obj, dir, storagePath, schemaPath)
}

// AttributeResource manages attribute operations for a collection
type AttributeResource struct {
	reqCtx RequestContext
}

// NewAttributeResource creates a new AttributeResource instance
func NewAttributeResource(ctx context.Context, reqCtx RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if reqCtx.Catalog == "" || reqCtx.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if reqCtx.Variant == "" || reqCtx.VariantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	return &AttributeResource{
		reqCtx: reqCtx,
	}, nil
}

type collectionSchemaRef struct {
	name string
	path string
}

// Get retrieves attribute values from a collection
func (ar *AttributeResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	var returnCollection bool
	collectionSchema := collectionSchemaRef{}
	if ar.reqCtx.QueryParams.Get("collection") == "true" {
		returnCollection = true
		collectionSchema.name = ar.reqCtx.ObjectName
		collectionSchema.path = ar.reqCtx.ObjectPath
	} else {
		returnCollection = false
		collectionSchema.name = path.Base(ar.reqCtx.ObjectPath)
		collectionSchema.path = path.Dir(ar.reqCtx.ObjectPath)
	}

	metadata := &schemamanager.SchemaMetadata{
		Catalog:   ar.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(ar.reqCtx.Variant),
		Namespace: types.NullableStringFrom(ar.reqCtx.Namespace),
		Path:      collectionSchema.path,
		Name:      collectionSchema.name,
	}
	if err := metadata.Validate(); err != nil {
		return nil, ErrInvalidSchema.Msg("invalid resource path")
	}
	metadata.IDS.CatalogID = ar.reqCtx.CatalogID
	metadata.IDS.VariantID = ar.reqCtx.VariantID

	if !returnCollection {
		value, err := GetAttribute(ctx, metadata, ar.reqCtx.ObjectName, WithWorkspaceID(ar.reqCtx.WorkspaceID))
		if err != nil {
			return nil, err
		}
		response := map[string]json.RawMessage{
			ar.reqCtx.ObjectName: value,
		}
		if ret, err := json.Marshal(&response); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal attribute")
			return nil, ErrUnableToLoadObject.Msg("unable to load attribute")
		} else {
			return ret, nil
		}
	} else {
		values, err := GetAllAttributes(ctx, metadata, WithWorkspaceID(ar.reqCtx.WorkspaceID))
		if err != nil {
			return nil, err
		}
		response := map[string]json.RawMessage{
			"values": values,
		}
		if ret, err := json.Marshal(&response); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal attribute")
			return nil, ErrUnableToLoadObject.Msg("unable to load attribute")
		} else {
			return ret, nil
		}
	}
}

// Update modifies attribute values in a collection
func (ar *AttributeResource) Update(ctx context.Context, resourceJSON []byte) apperrors.Error {
	var updateCollection bool
	if gjson.GetBytes(resourceJSON, "value").Exists() {
		updateCollection = false
	} else if gjson.GetBytes(resourceJSON, "values").Exists() {
		updateCollection = true
	} else {
		return validationerrors.ErrSchemaValidation.Msg("invalid request")
	}

	collectionSchema := collectionSchemaRef{}
	if updateCollection {
		collectionSchema.name = ar.reqCtx.ObjectName
		collectionSchema.path = ar.reqCtx.ObjectPath
	} else {
		collectionSchema.name = path.Base(ar.reqCtx.ObjectPath)
		collectionSchema.path = path.Dir(ar.reqCtx.ObjectPath)
	}

	metadata := &schemamanager.SchemaMetadata{
		Catalog:   ar.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(ar.reqCtx.Variant),
		Namespace: types.NullableStringFrom(ar.reqCtx.Namespace),
		Path:      collectionSchema.path,
		Name:      collectionSchema.name,
	}
	if err := metadata.Validate(); err != nil {
		return validationerrors.ErrSchemaValidation.Msg(err.Error())
	}
	metadata.IDS.CatalogID = ar.reqCtx.CatalogID
	metadata.IDS.VariantID = ar.reqCtx.VariantID

	if updateCollection {
		valuesJSON := gjson.GetBytes(resourceJSON, "values")
		if !valuesJSON.Exists() {
			return validationerrors.ErrSchemaValidation.Msg("invalid request")
		}
		values := make(AttributeValues)
		if err := json.Unmarshal([]byte(valuesJSON.Raw), &values); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
			return validationerrors.ErrSchemaValidation.Msg("failed to parse request")
		}
		if len(values) > 0 {
			return UpdateAttributes(ctx, metadata, values, WithWorkspaceID(ar.reqCtx.WorkspaceID))
		}
	} else {
		valueJSON := gjson.GetBytes(resourceJSON, "value")
		if !valueJSON.Exists() {
			return validationerrors.ErrSchemaValidation.Msg("invalid request")
		}
		values := make(AttributeValues)
		values[ar.reqCtx.ObjectName] = types.NullableAnySetRaw([]byte(valueJSON.Raw))
		return UpdateAttributes(ctx, metadata, values, WithWorkspaceID(ar.reqCtx.WorkspaceID))
	}
	return nil
}

// Delete removes an attribute from a collection
func (ar *AttributeResource) Delete(ctx context.Context) apperrors.Error {
	collectionSchema := collectionSchemaRef{}
	collectionSchema.name = path.Base(ar.reqCtx.ObjectPath)
	collectionSchema.path = path.Dir(ar.reqCtx.ObjectPath)

	metadata := &schemamanager.SchemaMetadata{
		Catalog:   ar.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(ar.reqCtx.Variant),
		Namespace: types.NullableStringFrom(ar.reqCtx.Namespace),
		Path:      collectionSchema.path,
		Name:      collectionSchema.name,
	}
	if err := metadata.Validate(); err != nil {
		return ErrInvalidSchema.Msg("invalid resource path")
	}
	metadata.IDS.CatalogID = ar.reqCtx.CatalogID
	metadata.IDS.VariantID = ar.reqCtx.VariantID

	attrName := ar.reqCtx.ObjectName

	return DeleteAttribute(ctx, metadata, attrName, WithWorkspaceID(ar.reqCtx.WorkspaceID))
}

// Location returns the resource location
func (ar *AttributeResource) Location() string {
	return ""
}

// Create is not supported for attributes
func (ar *AttributeResource) Create(ctx context.Context, resourceJSON []byte) (string, apperrors.Error) {
	return ar.Location(), ErrInvalidRequest
}
