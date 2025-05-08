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

func GetAttribute(ctx context.Context, m *schemamanager.SchemaMetadata, param string, opts ...ObjectStoreOption) ([]byte, apperrors.Error) {
	if m == nil || param == "" {
		return nil, validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	collection, err := loadCollectionObjectByPath(ctx, m, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Msg("collection not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return nil, err
		}
	}
	var cm schemamanager.CollectionManager
	cm, err = collectionManagerFromObject(ctx, collection, m)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection")
		return nil, err
	}

	return cm.GetValueJSON(ctx, param)
}

func GetAllAttributes(ctx context.Context, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) ([]byte, apperrors.Error) {
	if m == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	collection, err := loadCollectionObjectByPath(ctx, m, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Msg("collection not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return nil, err
		}
	}
	var cm schemamanager.CollectionManager
	cm, err = collectionManagerFromObject(ctx, collection, m)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection")
		return nil, err
	}

	return cm.GetAllValuesJSON(ctx)
}

func DeleteAttribute(ctx context.Context, m *schemamanager.SchemaMetadata, param string, opts ...ObjectStoreOption) apperrors.Error {
	if m == nil || param == "" {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var dir Directories
	t := types.CatalogObjectTypeCatalogCollection
	pathWithName := path.Clean(m.GetStoragePath(t) + "/" + m.Name)

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getWorkspaceDirs(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, m.IDS.VariantID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	existingCollection, err := loadCollectionObjectByPath(ctx, m, opts...)
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
		cm, err = collectionManagerFromObject(ctx, existingCollection, m)
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

	err = cm.SetDefaultValues(param)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set default values")
		return err
	}

	s := cm.StorageRepresentation()
	data, err := s.Serialize()
	if err != nil {
		return err
	}
	newHash := s.GetHash()
	if existingCollection != nil && newHash == existingCollection.Hash {
		if options.ErrorIfEqualToExisting {
			return ErrEqualToExistingObject
		}
		return nil
	}

	// store this object and update the reference
	obj := models.CatalogObject{
		Type:    types.CatalogObjectTypeCatalogCollection,
		Hash:    newHash,
		Version: s.Version,
		Data:    data,
	}

	return saveCollectionObject(ctx, m, &obj, dir, pathWithName, schemaPath)
}

type attributeValues map[string]types.NullableAny

func UpdateAttributes(ctx context.Context, m *schemamanager.SchemaMetadata, values attributeValues, opts ...ObjectStoreOption) apperrors.Error {
	if m == nil || values == nil {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var dir Directories
	t := types.CatalogObjectTypeCatalogCollection
	pathWithName := path.Clean(m.GetStoragePath(t) + "/" + m.Name)

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getWorkspaceDirs(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, m.IDS.VariantID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	existingCollection, err := loadCollectionObjectByPath(ctx, m, opts...)
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
		cm, err = collectionManagerFromObject(ctx, existingCollection, m)
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

	for param, value := range values {
		v, _ := cm.GetValue(ctx, param)
		if v.Equals(value) {
			log.Ctx(ctx).Info().Msg("value is the same, no update needed")
			continue
		}

		err = cm.SetValue(ctx, schemaLoaders, param, value)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to set value in collection manager")
			return err
		}
	}

	s := cm.StorageRepresentation()
	data, err := s.Serialize()
	if err != nil {
		return err
	}
	newHash := s.GetHash()
	if existingCollection != nil && newHash == existingCollection.Hash {
		if options.ErrorIfEqualToExisting {
			return ErrEqualToExistingObject
		}
		return nil
	}

	// store this object and update the reference
	obj := models.CatalogObject{
		Type:    types.CatalogObjectTypeCatalogCollection,
		Hash:    newHash,
		Version: s.Version,
		Data:    data,
	}

	return saveCollectionObject(ctx, m, &obj, dir, pathWithName, schemaPath)
}

type attributeResource struct {
	reqCtx RequestContext
}

func NewAttributeResource(ctx context.Context, reqCtx RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if reqCtx.Catalog == "" || reqCtx.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if reqCtx.Variant == "" || reqCtx.VariantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	return &attributeResource{
		reqCtx: reqCtx,
	}, nil
}

type collectionSchemaRef struct {
	name string
	path string
}

func (ar *attributeResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
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
	var _ = returnCollection
	m := &schemamanager.SchemaMetadata{
		Catalog:   ar.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(ar.reqCtx.Variant),
		Namespace: types.NullableStringFrom(ar.reqCtx.Namespace),
		Path:      collectionSchema.path,
		Name:      collectionSchema.name,
	}
	ves := m.Validate()
	if ves != nil {
		return nil, ErrInvalidSchema.Msg("invalid resource path")
	}
	m.IDS.CatalogID = ar.reqCtx.CatalogID
	m.IDS.VariantID = ar.reqCtx.VariantID

	if !returnCollection {
		object, err := GetAttribute(ctx, m, ar.reqCtx.ObjectName, WithWorkspaceID(ar.reqCtx.WorkspaceID))
		if err != nil {
			return nil, err
		}
		val := map[string]json.RawMessage{
			ar.reqCtx.ObjectName: object,
		}
		if ret, err := json.Marshal(&val); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal attribute")
			return nil, ErrUnableToLoadObject.Msg("unable to load attribute")
		} else {
			return ret, nil
		}
	} else {
		object, err := GetAllAttributes(ctx, m, WithWorkspaceID(ar.reqCtx.WorkspaceID))
		if err != nil {
			return nil, err
		}
		val := map[string]json.RawMessage{
			"values": object,
		}
		if ret, err := json.Marshal(&val); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to marshal attribute")
			return nil, ErrUnableToLoadObject.Msg("unable to load attribute")
		} else {
			return ret, nil
		}
	}
}

func (ar *attributeResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	var updateCollection bool
	if gjson.GetBytes(rsrcJson, "value").Exists() {
		updateCollection = false
	} else if gjson.GetBytes(rsrcJson, "values").Exists() {
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

	m := &schemamanager.SchemaMetadata{
		Catalog:   ar.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(ar.reqCtx.Variant),
		Namespace: types.NullableStringFrom(ar.reqCtx.Namespace),
		Path:      collectionSchema.path,
		Name:      collectionSchema.name,
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	m.IDS.CatalogID = ar.reqCtx.CatalogID
	m.IDS.VariantID = ar.reqCtx.VariantID

	if updateCollection {
		r := gjson.GetBytes(rsrcJson, "values")
		if !r.Exists() {
			return validationerrors.ErrSchemaValidation.Msg("invalid request")
		}
		values := make(attributeValues)
		if err := json.Unmarshal([]byte(r.Raw), &values); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
			return validationerrors.ErrSchemaValidation.Msg("failed to parse request")
		}
		if len(values) > 0 {
			return UpdateAttributes(ctx, m, values, WithWorkspaceID(ar.reqCtx.WorkspaceID))
		}
	} else {
		value := gjson.GetBytes(rsrcJson, "value")
		if !value.Exists() {
			return validationerrors.ErrSchemaValidation.Msg("invalid request")
		}
		values := make(attributeValues)
		values[ar.reqCtx.ObjectName] = types.NullableAnySetRaw([]byte(value.Raw))
		return UpdateAttributes(ctx, m, values, WithWorkspaceID(ar.reqCtx.WorkspaceID))
	}
	return nil
}

func (ar *attributeResource) Delete(ctx context.Context) apperrors.Error {
	collectionSchema := collectionSchemaRef{}
	collectionSchema.name = path.Base(ar.reqCtx.ObjectPath)
	collectionSchema.path = path.Dir(ar.reqCtx.ObjectPath)

	m := &schemamanager.SchemaMetadata{
		Catalog:   ar.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(ar.reqCtx.Variant),
		Namespace: types.NullableStringFrom(ar.reqCtx.Namespace),
		Path:      collectionSchema.path,
		Name:      collectionSchema.name,
	}
	ves := m.Validate()
	if ves != nil {
		return ErrInvalidSchema.Msg("invalid resource path")
	}
	m.IDS.CatalogID = ar.reqCtx.CatalogID
	m.IDS.VariantID = ar.reqCtx.VariantID

	param := ar.reqCtx.ObjectName

	return DeleteAttribute(ctx, m, param, WithWorkspaceID(ar.reqCtx.WorkspaceID))
}

func (ar *attributeResource) Location() string {
	return ""
}

func (ar *attributeResource) Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error) {
	return ar.Location(), ErrInvalidRequest
}
