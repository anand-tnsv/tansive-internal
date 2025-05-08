package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
)

func NewCollectionManager(ctx context.Context, rsrcJson []byte, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	// get the metadata, replace fields in json from provided metadata. Set defaults.
	rsrcJson, m, err := canonicalizeMetadata(rsrcJson, types.CollectionKind, m)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}

	var cs collectionSchema
	if err := json.Unmarshal(rsrcJson, &cs); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return nil, validationerrors.ErrSchemaValidation
	}
	ves := cs.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	// validate the metadata
	if err := validateMetadata(ctx, m); err != nil {
		return nil, err
	}

	cs.Metadata = *m

	return &collectionManager{
		schema: cs,
	}, nil
}

func SaveCollection(ctx context.Context, cm schemamanager.CollectionManager, opts ...ObjectStoreOption) apperrors.Error {
	if cm == nil {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	t := types.CatalogObjectTypeCatalogCollection
	var dir Directories
	rsrcPath := cm.Metadata().GetStoragePath(t)
	pathWithName := path.Clean(rsrcPath + "/" + cm.Metadata().Name)

	m := cm.Metadata()
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

	existingCollection, err := loadCollectionObjectByPath(ctx, &m, opts...)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			existingCollection = nil
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return err
		}
	}

	var cmCurrent schemamanager.CollectionManager
	if existingCollection != nil {
		if options.ErrorIfExists {
			return ErrAlreadyExists.Msg("collection already exists")
		}
		cmCurrent, err = collectionManagerFromObject(ctx, existingCollection, &m)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to load existing collection")
			return err
		}
		// collection cannot be modified if schema is different
		if cmCurrent.Schema() != cm.Schema() {
			return ErrSchemaOfCollectionNotMutable
		}
	}

	schemaPath, schemaLoaders, err := setCollectionSchemaManager(ctx, cm, dir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set collection schema manager")
		return err
	}
	cm.SetCollectionSchemaPath(schemaPath)

	var cmCurrentValues schemamanager.ParamValues = nil
	if cmCurrent != nil {
		cmCurrentValues = cmCurrent.Values()
	}

	if err := cm.ValidateValues(ctx, schemaLoaders, cmCurrentValues); err != nil {
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
		Type:    t,
		Hash:    newHash,
		Version: s.Version,
		Data:    data,
	}

	return saveCollectionObject(ctx, &m, &obj, dir, pathWithName, schemaPath)
}

func setCollectionSchemaManager(ctx context.Context, cm schemamanager.CollectionManager, dir Directories) (string, schemamanager.SchemaLoaders, apperrors.Error) {
	var schemaPath string
	var schemaObj *models.ObjectRef
	var err apperrors.Error
	var schemaLoaders schemamanager.SchemaLoaders

	// Now we try for the schema either in the namespace cr in the root namespace
	schemaPath = cm.GetCollectionSchemaPath()
	if schemaPath != "" {
		schemaObj, err = db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, schemaPath)
	} else {
		m := cm.Metadata()
		if !cm.Metadata().Namespace.IsNil() {
			schemaPath = path.Clean(m.GetStoragePath(types.CatalogObjectTypeCollectionSchema) + "/" + cm.Schema())
			schemaObj, err = db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, schemaPath)
		}
		if schemaObj == nil {
			m.Namespace = types.NullString()
			schemaPath = path.Clean(m.GetStoragePath(types.CatalogObjectTypeCollectionSchema) + "/" + cm.Schema())
			schemaObj, err = db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, schemaPath)
		}
	}

	if err != nil || schemaObj == nil {
		return schemaPath, schemaLoaders, ErrInvalidCollectionSchema
	}

	if err := loadCollectionSchemaManager(ctx, schemaObj.Hash, cm, WithDirectories(dir)); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection schema manager")
		return schemaPath, schemaLoaders, err
	}

	schemaLoaders = getSchemaLoaders(ctx, cm.Metadata(), WithDirectories(dir), SkipCanonicalizePaths())
	schemaLoaders.ParameterRef = func(name string) string {
		for _, ref := range schemaObj.References {
			if name == path.Base(ref.Name) {
				return ref.Name
			}
		}
		return ""
	}
	return schemaPath, schemaLoaders, nil
}

func saveCollectionObject(ctx context.Context, m *schemamanager.SchemaMetadata, obj *models.CatalogObject, dir Directories, pathWithName, collectionSchema string) apperrors.Error {
	dberr := db.DB(ctx).CreateCatalogObject(ctx, obj)
	if dberr != nil {
		if errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
			return nil // already exists, nothing to do
		}
		log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
		return dberr
	}

	repoId := uuid.Nil
	if dir.WorkspaceID != uuid.Nil {
		repoId = dir.WorkspaceID
	} else if dir.VariantID != uuid.Nil {
		repoId = dir.VariantID
	}

	namespace := m.Namespace.String()
	if namespace == "" {
		namespace = types.DefaultNamespace
	}

	c := models.Collection{
		Path:             pathWithName,
		Hash:             obj.Hash,
		CollectionSchema: collectionSchema,
		RepoID:           repoId,
		VariantID:        m.IDS.VariantID,
	}

	if err := db.DB(ctx).UpsertCollection(ctx, &c, dir.ValuesDir); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create collection in database")
		// If the collection creation fails, we should also delete the catalog object
		if _, delErr := db.DB(ctx).DeleteObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir.ValuesDir, pathWithName); delErr != nil {
			log.Ctx(ctx).Error().Err(delErr).Msg("failed to delete catalog object after collection creation failure")
		}
		return ErrCatalogError.Err(err)
	}
	/*
		// the reference will point to the collection schema
		var refModel models.References
		refModel = append(refModel, models.Reference{
			Name: collectionSchema,
		})

		// store the reference in the directory
		if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir.ValuesDir, pathWithName, models.ObjectRef{
			Hash:       obj.Hash,
			References: refModel,
		}); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
			return ErrCatalogError
		}
	*/
	return nil
}

func DeleteCollection(ctx context.Context, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) apperrors.Error {
	if m == nil {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	t := types.CatalogObjectTypeCatalogCollection
	rsrcPath := m.GetStoragePath(t)
	pathWithName := path.Clean(rsrcPath + "/" + m.Name)

	// get the directory
	var dir Directories
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

	hash, err := db.DB(ctx).DeleteCollection(ctx, pathWithName, dir.ValuesDir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete collection")
		if errors.Is(err, dberror.ErrNotFound) {
			return nil // already deleted
		}
		return err
	}

	if hash != "" {
		err = db.DB(ctx).DeleteCatalogObject(ctx, types.CatalogObjectTypeCatalogCollection, hash)
		if !errors.Is(err, dberror.ErrNotFound) {
			// we don't return an error since the object reference has already been removed and
			// we cannot roll this back.
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete object from database")
		}
	}

	return nil
}

func LoadCollectionByHash(ctx context.Context, hash string, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.CollectionManager, apperrors.Error) {
	if hash == "" {
		return nil, validationerrors.ErrEmptySchema
	}
	o := storeOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	obj, err := db.DB(ctx).GetCatalogObject(ctx, hash)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Err(err)
		}
		return nil, ErrUnableToLoadObject.Err(err)
	}

	if obj.Type != types.CatalogObjectTypeCatalogCollection {
		log.Ctx(ctx).Error().Msg("invalid collection type")
		return nil, ErrUnableToLoadObject
	}

	return collectionManagerFromObject(ctx, obj, m)
}

func loadCollectionObjectByPath(ctx context.Context, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (*models.CatalogObject, apperrors.Error) {
	if m == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	t := types.CatalogObjectTypeCatalogCollection
	rsrcPath := m.GetStoragePath(t)
	pathWithName := path.Clean(rsrcPath + "/" + m.Name)

	// get the directory
	var dir Directories
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getWorkspaceDirs(ctx, options.WorkspaceID)
		if err != nil {
			return nil, err
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, m.IDS.VariantID)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrInvalidVersionOrWorkspace
	}

	// get the collection from DB
	obj, err := db.DB(ctx).GetCollectionObject(ctx, pathWithName, dir.ValuesDir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object by path")
		return nil, err
	}

	return obj, nil
}

func LoadCollectionByPath(ctx context.Context, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.CollectionManager, apperrors.Error) {
	obj, err := loadCollectionObjectByPath(ctx, m, opts...)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object by path")
		return nil, err
	}
	cm, err := collectionManagerFromObject(ctx, obj, m)
	return cm, err
}

func loadCollectionSchemaManager(ctx context.Context, hash string, cm schemamanager.CollectionManager, opts ...ObjectStoreOption) apperrors.Error {
	m := &schemamanager.SchemaMetadata{
		Name:    cm.Schema(),
		Catalog: cm.Metadata().Catalog,
		Variant: cm.Metadata().Variant,
	}
	sm, err := GetSchemaByHash(ctx, hash, m, opts...)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection schema manager")
		return err
	}
	cm.SetCollectionSchemaManager(sm.CollectionSchemaManager())
	return nil
}

func collectionManagerFromObject(ctx context.Context, obj *models.CatalogObject, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if obj == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	s := schemastore.SchemaStorageRepresentation{}
	if err := json.Unmarshal(obj.Data, &s); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema")
		return nil, validationerrors.ErrSchemaValidation
	}
	if s.Type != types.CatalogObjectTypeCatalogCollection {
		log.Ctx(ctx).Error().Msg("invalid collection schema type")
		return nil, ErrUnableToLoadObject
	}
	if s.Type != types.CatalogObjectTypeCatalogCollection {
		log.Ctx(ctx).Error().Msg("invalid collection schema kind")
		return nil, ErrUnableToLoadObject
	}

	cm := &collectionManager{}
	if err := json.Unmarshal(s.Schema, &cm.schema.Spec); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema spec")
		return nil, ErrUnableToLoadObject
	}
	if err := json.Unmarshal(s.Values, &cm.schema.Values); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema values")
		return nil, ErrUnableToLoadObject
	}
	cm.schema.Kind = types.CollectionKind
	cm.schema.Version = s.Version
	cm.schema.Metadata = *m
	var schemaPathNS types.NullableString
	err := json.Unmarshal(s.Reserved, &schemaPathNS)
	if err == nil {
		cm.schema.SchemaPath = schemaPathNS.String()
	}
	return cm, nil
}

type collectionResource struct {
	reqCtx RequestContext
	cm     schemamanager.CollectionManager
}

func (cr *collectionResource) Name() string {
	return cr.reqCtx.ObjectName
}

func (cr *collectionResource) Location() string {
	objName := types.ResourceNameFromObjectType(cr.reqCtx.ObjectType)
	loc := path.Clean("/" + objName + cr.cm.FullyQualifiedName())
	q := url.Values{}
	if workspace := cr.reqCtx.WorkspaceLabel; workspace != "" {
		q.Set("workspace", workspace)
	}
	if namespace := cr.cm.Metadata().Namespace.String(); namespace != "" {
		q.Set("namespace", namespace)
	}
	qStr := q.Encode()
	if qStr != "" {
		loc += "?" + qStr
	}
	return loc
}

func (cr *collectionResource) Manager() schemamanager.CollectionManager {
	return cr.cm
}

func (cr *collectionResource) Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   cr.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(cr.reqCtx.Variant),
		Namespace: types.NullableStringFrom(cr.reqCtx.Namespace),
	}

	collection, err := NewCollectionManager(ctx, rsrcJson, m)
	if err != nil {
		return "", err
	}
	err = SaveCollection(ctx, collection, WithWorkspaceID(cr.reqCtx.WorkspaceID), WithErrorIfExists())
	if err != nil {
		return "", err
	}

	cr.reqCtx.ObjectName = collection.Metadata().Name
	cr.reqCtx.ObjectPath = collection.Metadata().Path
	cr.reqCtx.ObjectType = types.CatalogObjectTypeCatalogCollection
	cr.cm = collection

	if cr.reqCtx.Catalog == "" {
		cr.reqCtx.Catalog = collection.Metadata().Catalog
	}
	if cr.reqCtx.Variant == "" {
		cr.reqCtx.Variant = collection.Metadata().Variant.String()
	}
	if cr.reqCtx.Namespace == "" {
		cr.reqCtx.Namespace = collection.Metadata().Namespace.String()
	}

	return cr.Location(), nil
}

func (cr *collectionResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   cr.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(cr.reqCtx.Variant),
		Namespace: types.NullableStringFrom(cr.reqCtx.Namespace),
		Path:      cr.reqCtx.ObjectPath,
		Name:      cr.reqCtx.ObjectName,
	}
	ves := m.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	m.IDS.CatalogID = cr.reqCtx.CatalogID
	m.IDS.VariantID = cr.reqCtx.VariantID

	object, err := LoadCollectionByPath(ctx, m, WithWorkspaceID(cr.reqCtx.WorkspaceID))
	if err != nil {
		return nil, err
	}
	return object.ToJson(ctx)
}

func (cr *collectionResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	if cr.reqCtx.WorkspaceID == uuid.Nil && cr.reqCtx.VariantID == uuid.Nil {
		return ErrInvalidWorkspaceOrVariant
	}
	var dir Directories
	var err apperrors.Error
	if cr.reqCtx.WorkspaceID != uuid.Nil {
		dir, err = getWorkspaceDirs(ctx, cr.reqCtx.WorkspaceID)
	} else {
		dir, err = getVariantDirs(ctx, cr.reqCtx.VariantID)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("workspace_id", cr.reqCtx.WorkspaceID.String()).Str("variant_id", cr.reqCtx.VariantID.String()).Msg("failed to get directories")
		return ErrInvalidWorkspaceOrVariant
	}

	m := &schemamanager.SchemaMetadata{
		Catalog:   cr.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(cr.reqCtx.Variant),
		Path:      cr.reqCtx.ObjectPath,
		Name:      cr.reqCtx.ObjectName,
		Namespace: types.NullableStringFrom(cr.reqCtx.Namespace),
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	m.IDS.CatalogID = cr.reqCtx.CatalogID
	m.IDS.VariantID = cr.reqCtx.VariantID

	// Load the existing object
	existing, err := LoadCollectionByPath(ctx, m, WithDirectories(dir))
	if err != nil {
		return err
	}
	if existing == nil {
		return ErrObjectNotFound
	}

	collection, err := NewCollectionManager(ctx, rsrcJson, m)
	if err != nil {
		return err
	}
	err = SaveCollection(ctx, collection, WithWorkspaceID(cr.reqCtx.WorkspaceID))
	if err != nil {
		return err
	}

	return nil
}

func (cr *collectionResource) Delete(ctx context.Context) apperrors.Error {
	if cr.reqCtx.WorkspaceID == uuid.Nil && cr.reqCtx.VariantID == uuid.Nil {
		return ErrInvalidWorkspace
	}
	var dir Directories
	var err apperrors.Error
	if cr.reqCtx.WorkspaceID != uuid.Nil {
		dir, err = getWorkspaceDirs(ctx, cr.reqCtx.WorkspaceID)
	} else {
		dir, err = getVariantDirs(ctx, cr.reqCtx.VariantID)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("workspace_id", cr.reqCtx.WorkspaceID.String()).Str("variant_id", cr.reqCtx.VariantID.String()).Msg("failed to get directories")
		return ErrInvalidWorkspace
	}
	m := &schemamanager.SchemaMetadata{
		Catalog:   cr.reqCtx.Catalog,
		Variant:   types.NullableStringFrom(cr.reqCtx.Variant),
		Path:      cr.reqCtx.ObjectPath,
		Name:      cr.reqCtx.ObjectName,
		Namespace: types.NullableStringFrom(cr.reqCtx.Namespace),
	}
	pathWithName := path.Clean(m.GetStoragePath(cr.reqCtx.ObjectType) + "/" + cr.reqCtx.ObjectName)
	err = DeleteCollection(ctx, m, WithDirectories(dir))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("failed to delete object")
		return err
	}
	return nil
}

func NewCollectionResource(ctx context.Context, reqCtx RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if reqCtx.Catalog == "" || reqCtx.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if reqCtx.Variant == "" || reqCtx.VariantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	return &collectionResource{
		reqCtx: reqCtx,
	}, nil
}
