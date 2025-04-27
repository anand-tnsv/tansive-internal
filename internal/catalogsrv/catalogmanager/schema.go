package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	v1Schema "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/schemaresource"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
)

type VersionHeader struct {
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

func NewSchema(ctx context.Context, rsrcJson []byte, m *schemamanager.SchemaMetadata) (schemamanager.SchemaManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}
	// get the version
	var version VersionHeader
	err := json.Unmarshal(rsrcJson, &version)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal version header")
		return nil, validationerrors.ErrSchemaValidation
	}
	if version.Version == "" {
		return nil, validationerrors.ErrSchemaValidation.Msg(schemaerr.ErrMissingRequiredAttribute("version").Error())
	}

	// validate the version
	if version.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}

	// get the metadata, replace fields in json from provided metadata. Set defaults.
	rsrcJson, m, err = canonicalizeMetadata(rsrcJson, version.Kind, m)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}

	// validate the metadata
	if err := validateMetadata(ctx, m); err != nil {
		return nil, err
	}

	var sm schemamanager.SchemaManager
	var apperr apperrors.Error
	if sm, apperr = v1Schema.NewV1SchemaManager(ctx, rsrcJson, schemamanager.WithValidation(), schemamanager.WithDefaultValues()); apperr != nil {
		return nil, apperr
	} else {
		sm.SetMetadata(m)
	}

	return sm, apperr
}

type storeOptions struct {
	ErrorIfExists                  bool
	ErrorIfEqualToExisting         bool
	WorkspaceID                    uuid.UUID
	Dir                            Directories
	SetDefaultValues               bool
	SkipValidationForUpdate        bool
	SkipCanonicalizePaths          bool
	IgnoreSchemaSpecChange         bool
	SkipRevalidationOnSchemaChange bool
	VersionNum                     int
}

type Directories struct {
	ParametersDir  uuid.UUID
	CollectionsDir uuid.UUID
	ValuesDir      uuid.UUID
	WorkspaceID    uuid.UUID
	VariantID      uuid.UUID
}

func (d Directories) IsNil() bool {
	return d.ParametersDir == uuid.Nil && d.CollectionsDir == uuid.Nil && d.ValuesDir == uuid.Nil
}

func (d Directories) DirForType(t types.CatalogObjectType) uuid.UUID {
	switch t {
	case types.CatalogObjectTypeParameterSchema:
		return d.ParametersDir
	case types.CatalogObjectTypeCollectionSchema:
		return d.CollectionsDir
	case types.CatalogObjectTypeCatalogCollection:
		return d.ValuesDir
	default:
		return uuid.Nil
	}
}

type ObjectStoreOption func(*storeOptions)

func WithErrorIfExists() ObjectStoreOption {
	return func(o *storeOptions) {
		o.ErrorIfExists = true
	}
}

func WithErrorIfEqualToExisting() ObjectStoreOption {
	return func(o *storeOptions) {
		o.ErrorIfEqualToExisting = true
	}
}

func WithWorkspaceID(id uuid.UUID) ObjectStoreOption {
	return func(o *storeOptions) {
		o.WorkspaceID = id
	}
}

func WithDirectories(d Directories) ObjectStoreOption {
	return func(o *storeOptions) {
		o.Dir = d
	}
}

func WithVersionNum(num int) ObjectStoreOption {
	return func(o *storeOptions) {
		o.VersionNum = num
	}
}

func SkipValidationForUpdate() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipValidationForUpdate = true
	}
}

func SetDefaultValues() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SetDefaultValues = true
	}
}

func SkipCanonicalizePaths() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipCanonicalizePaths = true
	}
}

func IgnoreSchemaSpecChange() ObjectStoreOption {
	return func(o *storeOptions) {
		o.IgnoreSchemaSpecChange = true
	}
}

func SkipRevalidationOnSchemaChange() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipRevalidationOnSchemaChange = true
	}
}

func SaveSchema(ctx context.Context, om schemamanager.SchemaManager, opts ...ObjectStoreOption) apperrors.Error {
	if om == nil {
		return validationerrors.ErrEmptySchema
	}

	m := om.Metadata()

	// get the options
	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var (
		t                  types.CatalogObjectType = om.Type()           // object type
		dir                Directories                                   // directories for this object type
		hash               string                                        // hash of the object's storage representation
		rsrcPath           string                  = m.GetStoragePath(t) // path to the object in the directory
		pathWithName       string                  = ""                  // fully qualified resource path with name
		refs, existingRefs schemamanager.SchemaReferences
		existingParamPath  string
		existingParamRef   *models.ObjectRef
		existingObjHash    string // Hash of the existing object with same path in the directory
	)

	// strip path with any trailing slashes and append the name to get a FQRP
	pathWithName = path.Clean(rsrcPath + "/" + m.Name)

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForWorkspace(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForVariant(ctx, m.IDS.VariantID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	switch t {
	case types.CatalogObjectTypeParameterSchema:
		if options.SkipValidationForUpdate {
			break
		}
		var err apperrors.Error
		if existingObjHash, refs, existingParamPath, existingParamRef, err = validateParameterSchema(ctx, om, dir, options); err != nil {
			return err
		}
	case types.CatalogObjectTypeCollectionSchema:
		if options.SkipValidationForUpdate {
			break
		}
		var err apperrors.Error
		if existingObjHash, refs, existingRefs, err = validateCollectionSchema(ctx, om, dir, options.ErrorIfExists); err != nil {
			return err
		}
	default:
		return ErrCatalogError.Msg("invalid object type")
	}

	if om.Type() == types.CatalogObjectTypeCollectionSchema {
		om.CollectionSchemaManager().SetDefaultValues(ctx)
	}

	s := om.StorageRepresentation()
	if s == nil {
		return validationerrors.ErrEmptySchema
	}

	hash = s.GetHash()
	if hash == existingObjHash {
		if options.ErrorIfEqualToExisting {
			return ErrEqualToExistingObject
		}
		return nil
	}

	_ = existingParamRef
	_ = existingParamPath
	// if we came here, we have a new object to save
	data, err := s.Serialize()
	if err != nil {
		return validationerrors.ErrSchemaSerialization
	}

	obj := models.CatalogObject{
		Type:    s.Type,
		Version: s.Version,
		Data:    data,
		Hash:    hash,
	}

	// Save obj to the database
	dberr := db.DB(ctx).CreateCatalogObject(ctx, &obj)
	if dberr != nil {
		if errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
			// in this case, we don't return. If we came here it means the object is not in the directory,
			// so we'll keep chugging along and save the object to the directory
		} else {
			log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
			return dberr
		}
	}

	var refModel models.References
	for _, ref := range refs {
		refModel = append(refModel, models.Reference{
			Name: ref.Name,
		})
	}

	if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, t, dir.DirForType(t), pathWithName, models.ObjectRef{
		Hash:       obj.Hash,
		References: refModel,
	}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
		return ErrCatalogError
	}

	if t == types.CatalogObjectTypeCollectionSchema && !options.SkipValidationForUpdate {
		syncCollectionReferencesInParameters(ctx, dir.ParametersDir, pathWithName, existingRefs, refs)
	} else if t == types.CatalogObjectTypeParameterSchema && len(refs) > 0 {
		syncParameterReferencesInCollections(ctx, dir, existingParamPath, pathWithName, existingParamRef, refs)
	}

	return nil
}

func syncParameterReferencesInCollections(ctx context.Context, dir Directories, existingPath, newPath string, existingParamObjRef *models.ObjectRef, newCollectionRefs schemamanager.SchemaReferences) {
	var newRefsForExistingParam models.References
	if existingParamObjRef != nil {
		for _, ref := range existingParamObjRef.References {
			remove := false
			for _, newRef := range newCollectionRefs {
				if ref.Name == newRef.Name {
					remove = true
				}
			}
			if !remove {
				newRefsForExistingParam = append(newRefsForExistingParam, ref)
			}
		}
	}
	if len(newRefsForExistingParam) > 0 {
		existingParamObjRef.References = newRefsForExistingParam
		// save the updated references for the parameter
		if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, existingPath, *existingParamObjRef); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to update parameter references")
		}
	}

	// if there are no existing references, we don't need to do anything
	if existingParamObjRef == nil {
		return
	}

	// for all the collections that will now map to the new parameter, replace the old reference with the new one
	for _, newRef := range newCollectionRefs {
		if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, newRef.Name, []models.Reference{{Name: newPath}}); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to add new param path to collection")
		}
		if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, newRef.Name, existingPath); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to delete new param path from collection")
		}
	}
}

func syncCollectionReferencesInParameters(ctx context.Context, paramDir uuid.UUID, collectionFqp string, existingParamRefs, newParamRefs schemamanager.SchemaReferences) {
	type refAction string
	const (
		actionAdd    refAction = "add"
		actionDelete refAction = "delete"
	)

	refActions := make(map[string]refAction)

	// Mark new references for addition
	for _, newRef := range newParamRefs {
		refActions[newRef.Name] = actionAdd
	}

	// Handle existing references (remove or keep)
	for _, existingRef := range existingParamRefs {
		if _, ok := refActions[existingRef.Name]; !ok {
			refActions[existingRef.Name] = actionDelete
		} else {
			delete(refActions, existingRef.Name)
		}
	}

	// Execute actions
	for param, action := range refActions {
		switch action {
		case actionAdd:
			if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, []models.Reference{{Name: collectionFqp}}); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collectionschema", collectionFqp).
					Err(err).
					Msg("failed to add references to collection schema")
			}
		case actionDelete:
			if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, collectionFqp); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collectionschema", collectionFqp).
					Err(err).
					Msg("failed to delete references from collection schema")
			}
		}
	}
}

func validateParameterSchema(ctx context.Context, om schemamanager.SchemaManager, dir Directories, options storeOptions) (
	existingObjHash string,
	newRefs schemamanager.SchemaReferences,
	existingPath string,
	existingParamRef *models.ObjectRef,
	err apperrors.Error) {

	m := om.Metadata()
	pathWithName := path.Clean(m.GetStoragePath(types.CatalogObjectTypeParameterSchema) + "/" + m.Name)

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
			err = nil // no existing object found, so it's a new one
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			err = ErrCatalogError
			return
		}
	}

	if r != nil {
		if options.ErrorIfExists {
			err = ErrAlreadyExists
			return
		}
		if len(r.References) > 0 {
			// load from hash
			var sm schemamanager.SchemaManager
			sm, err = LoadSchemaByHash(ctx, r.Hash, &m)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to load existing parameter schema by hash")
				err = ErrUnableToSaveSchema
				return
			}
			pm := sm.ParameterSchemaManager()
			if pm == nil {
				log.Ctx(ctx).Error().Msg("loaded parameter schema manager is nil")
				err = ErrUnableToSaveSchema
				return
			}
			if !options.IgnoreSchemaSpecChange && om.ParameterSchemaManager().StorageRepresentation().DiffersInSpec(pm.StorageRepresentation()) {
				err = ErrSchemaConflict.Msg("cannot modify schema spec; one or more collection schemas refer to this parameter schema")
				return
			}
			for _, ref := range r.References {
				newRefs = append(newRefs, schemamanager.SchemaReference{
					Name: ref.Name,
				})
			}
		}
		existingObjHash = r.Hash
	} else {
		// check if there are existing parameters with the same name in this namespace or the root namespace
		existingPath, existingParamRef, err = db.DB(ctx).FindClosestObject(ctx,
			types.CatalogObjectTypeParameterSchema,
			dir.ParametersDir,
			m.Name,
			m.GetStoragePath(types.CatalogObjectTypeParameterSchema),
		)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("path", existingPath).Msg("failed to find closest object")
			err = ErrCatalogError.Msg("unable to save schema")
			return
		}
		if existingPath != "" && existingParamRef != nil {
			collectionRefs := existingParamRef.References
			var refsToAdd schemamanager.SchemaReferences
			for _, ref := range collectionRefs {
				if isParentOrSame(m.GetStoragePath(types.CatalogObjectTypeCollectionSchema), path.Dir(ref.Name)) {
					refsToAdd = append(refsToAdd, schemamanager.SchemaReference{
						Name: ref.Name,
					})
				}
			}
			if len(refsToAdd) > 0 {
				if config.RemapAttributeSchemaReferences {
					newRefs = append(newRefs, refsToAdd...)
				} else {
					err = ErrSchemaConflict.Msg("one or more collection schemas in this namespace refer to the same parameter schema in root namespace")
					return
				}
			}
		}
	}
	// if there are references to this object - either new or updated - we need to validate them
	if options.IgnoreSchemaSpecChange {
		if !options.SkipRevalidationOnSchemaChange && len(newRefs) > 0 {
			loaders := getSchemaLoaders(ctx, om.Metadata(), WithDirectories(dir))
			if pm := om.ParameterSchemaManager(); pm != nil {
				if err = pm.ValidateDependencies(ctx, loaders, newRefs); err != nil {
					return
				}
			}
		}
	}

	return
}

// isParentOrSame checks if p1 is a parent or the same as p2
func isParentOrSame(p1, p2 string) bool {
	// Clean paths to remove redundant elements
	p1 = path.Clean(p1)
	p2 = path.Clean(p2)

	// Check if p1 is a prefix of p2
	return p2 == p1 || strings.HasPrefix(p2, p1+"/")
}

// validateCollectionSchema ensures that all the dataTypes referenced by parameters in the Spec are valid.
// Similarly, it ensures that all the parameters referenced by the collection schema exist and also returns the
// references to the parameter schemas.
func validateCollectionSchema(ctx context.Context, om schemamanager.SchemaManager, dir Directories, errorIfExists bool) (
	existingObjHash string,
	newRefs schemamanager.SchemaReferences,
	existingRefs schemamanager.SchemaReferences,
	err apperrors.Error) {
	if om == nil {
		log.Ctx(ctx).Error().Msg("object manager is nil")
		err = ErrCatalogError
		return
	}

	cm := om.CollectionSchemaManager()
	if cm == nil {
		log.Ctx(ctx).Error().Msg("collection manager is nil")
		err = ErrCatalogError
		return
	}

	m := om.Metadata()
	parentPath := m.GetStoragePath(types.CatalogObjectTypeCollectionSchema)
	pathWithName := path.Clean(parentPath + "/" + m.Name)

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			err = ErrCatalogError
			return
		}
	}
	if r != nil {
		if errorIfExists {
			err = ErrAlreadyExists
			return
		}
		if len(r.References) > 0 {
			for _, ref := range r.References {
				existingRefs = append(existingRefs, schemamanager.SchemaReference{
					Name: ref.Name,
				})
			}
		}
		existingObjHash = r.Hash
	}
	/*
		else {
			// This is a new object. Check if the namespace exists
			//if err = collectionSchemaExists(ctx, dir.CollectionsDir, m.Path); err != nil {
			//	return
			//}
		}
	*/
	// validate the collection schema
	// The references are coming directly from store, and we don't want to canonicalize the paths
	loaders := getSchemaLoaders(ctx, m, WithDirectories(dir), SkipCanonicalizePaths())

	// refs are updated after validation
	if newRefs, err = cm.ValidateDependencies(ctx, loaders, existingRefs); err != nil {
		return
	}

	return
}

func deleteCollectionSchema(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, dir Directories) apperrors.Error {
	// check if there are references to this schema
	pathWithName := path.Clean(m.GetStoragePath(t) + "/" + m.Name)
	if m.IDS.VariantID == uuid.Nil {
		err := validateMetadata(ctx, m)
		if err != nil {
			return err
		}
	}

	exists, err := db.DB(ctx).HasReferencesToCollectionSchema(ctx, pathWithName, dir.ValuesDir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("failed to check if collection schema has references")
		return ErrCatalogError
	}
	if exists {
		log.Ctx(ctx).Info().Str("path", pathWithName).Msg("collection schema has references, cannot delete")
		return ErrUnableToDeleteCollectionWithReferences
	}

	// Remove all references in parameters and delete the object from the directory
	var hash string
	if hash, err = db.DB(ctx).DeleteObjectWithReferences(ctx,
		types.CatalogObjectTypeCollectionSchema,
		models.DirectoryIDs{
			{ID: dir.CollectionsDir, Type: types.CatalogObjectTypeCollectionSchema},
			{ID: dir.ParametersDir, Type: types.CatalogObjectTypeParameterSchema},
		},
		pathWithName,
		models.DeleteReferences(true),
	); err != nil {
		return ErrCatalogError.Err(err).Msg("unable to delete collection schema from directory")
	}
	// delete the object from the database
	if err := db.DB(ctx).DeleteCatalogObject(ctx, types.CatalogObjectTypeCollectionSchema, string(hash)); err != nil {
		if !errors.Is(err, dberror.ErrNotFound) {
			// we don't return an error since the object reference has already been removed and
			// we cannot roll this back.
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete object from database")
		}
	}
	return nil
}

func deleteParameterSchema(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, dir Directories) apperrors.Error {
	// check if there are references to this schema
	pathWithName := path.Clean(m.GetStoragePath(t) + "/" + m.Name)
	var hash types.Hash

	// if there are references to this schema, don't delete it.
	refs, err := db.DB(ctx).GetAllReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("failed to get all references")
		if errors.Is(err, dberror.ErrNotFound) {
			// no references found, safe to delete
			log.Ctx(ctx).Info().Str("path", pathWithName).Msg("no references found, safe to delete parameter schema")
		} else {
			return ErrCatalogError.Err(err).Msg("unable to delete parameter schema")
		}
	} else if len(refs) > 0 {
		log.Ctx(ctx).Info().Str("path", pathWithName).Msg("parameter schema has references, cannot delete")
		return ErrUnableToDeleteParameterWithReferences
	}

	// delete the object from the directory
	if hash, err = db.DB(ctx).DeleteObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName); err != nil {
		return ErrCatalogError.Err(err).Msg("unable to delete parameter schema from directory")
	}
	// delete the object from the database
	if err := db.DB(ctx).DeleteCatalogObject(ctx, types.CatalogObjectTypeParameterSchema, string(hash)); err != nil {
		if !errors.Is(err, dberror.ErrNotFound) {
			// we don't return an error since the object reference has already been removed and
			// we cannot roll this back.
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete objects from database")
		}
	}
	return nil
}

var _ = collectionSchemaExists

func collectionSchemaExists(ctx context.Context, collectionsDir uuid.UUID, path string) apperrors.Error {
	if path != "/" {
		var (
			exists bool
			err    apperrors.Error
		)
		if exists, err = db.DB(ctx).PathExists(ctx, types.CatalogObjectTypeCollectionSchema, collectionsDir, path); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("path", path).Msg("failed to check if parent path exists")
			return ErrCatalogError
		}
		if !exists {
			return ErrParentCollectionSchemaNotFound.Msg(path + " does not exist")
		}
	}
	return nil
}

func LoadSchemaByPath(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.SchemaManager, apperrors.Error) {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var dir uuid.UUID
	if !o.Dir.IsNil() && o.Dir.DirForType(t) != uuid.Nil {
		dir = o.Dir.DirForType(t)
	} else if o.WorkspaceID != uuid.Nil {
		dirs, err := getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if err != nil {
			return nil, err
		}
		dir = dirs.DirForType(t)
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dirs, err := getDirectoriesForVariant(ctx, m.IDS.VariantID)
		if err != nil {
			return nil, err
		}
		dir = dirs.DirForType(t)
	} else {
		return nil, ErrInvalidVersionOrWorkspace
	}

	rsrcPath := ""
	if o.SkipCanonicalizePaths {
		rsrcPath = m.Path + "/" + m.Name
	} else {
		rsrcPath = m.GetStoragePath(t) + "/" + m.Name
	}
	rsrcPath = path.Clean(rsrcPath)

	obj, err := db.DB(ctx).LoadObjectByPath(ctx, t, dir, rsrcPath)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, ErrCatalogError.Err(err)
	}
	if obj == nil { //should never get here
		return nil, ErrObjectNotFound
	}

	s := &schemastore.SchemaStorageRepresentation{}
	// we'll get the data from the object and not the table
	if err := json.Unmarshal(obj.Data, s); err != nil {
		return nil, ErrUnableToLoadObject.Err(err).Msg("failed to de-serialize catalog object data")
	}
	if s.Type != obj.Type {
		log.Ctx(ctx).Error().Str("Hash", obj.Hash).Msg("type mismatch when loading resource")
	}
	if s.Version != obj.Version {
		log.Ctx(ctx).Error().Str("Hash", obj.Hash).Msg("version mismatch when loading resource")
	}

	return v1Schema.LoadV1SchemaManager(ctx, s, m)
}

func DeleteSchema(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, dir Directories) apperrors.Error {
	if m == nil {
		return ErrEmptyMetadata
	}
	switch t {
	case types.CatalogObjectTypeCollectionSchema:
		return deleteCollectionSchema(ctx, t, m, dir)
	case types.CatalogObjectTypeParameterSchema:
		return deleteParameterSchema(ctx, t, m, dir)
	default:
		return ErrInvalidSchema
	}
}

func LoadSchemaByHash(ctx context.Context, hash string, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.SchemaManager, apperrors.Error) {
	if hash == "" {
		return nil, dberror.ErrInvalidInput.Msg("hash cannot be empty")
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

	s := &schemastore.SchemaStorageRepresentation{}
	// we'll get the data from the object and not the table
	if err := json.Unmarshal(obj.Data, s); err != nil {
		return nil, ErrUnableToLoadObject.Err(err).Msg("failed to de-serialize catalog object data")
	}
	if s.Type != obj.Type {
		log.Ctx(ctx).Error().Str("Hash", hash).Msg("type mismatch when loading resource")
	}
	if s.Version != obj.Version {
		log.Ctx(ctx).Error().Str("Hash", hash).Msg("version mismatch when loading resource")
	}

	return v1Schema.LoadV1SchemaManager(ctx, s, m)
}

func validateMetadata(ctx context.Context, m *schemamanager.SchemaMetadata) apperrors.Error {
	if m == nil {
		return ErrEmptyMetadata
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	var catalogId, variantId uuid.UUID
	// Check if the catalog exists
	if c, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, m.Catalog); err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrInvalidCatalog.Err(err)
		}
		return ErrCatalogError.Err(err)
	} else {
		catalogId = c.CatalogID
	}
	// check if the variant exists
	if !m.Variant.IsNil() {
		if v, err := db.DB(ctx).GetVariant(ctx, catalogId, uuid.Nil, m.Variant.String()); err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrVariantNotFound.Err(err)
			}
			return ErrCatalogError.Err(err)
		} else {
			variantId = v.VariantID
		}
	}
	// check if the namespace exists
	if !m.Namespace.IsNil() {
		if _, err := db.DB(ctx).GetNamespace(ctx, m.Namespace.String(), variantId); err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrNamespaceNotFound.Prefix(m.Namespace.String())
			}
			return ErrCatalogError.Err(err)
		}
	}
	// we won't handle resource path here
	m.IDS.CatalogID = catalogId
	m.IDS.VariantID = variantId
	return nil
}

func getClosestParentSchemaFinder(ctx context.Context, m schemamanager.SchemaMetadata, opts ...ObjectStoreOption) schemamanager.ClosestParentSchemaFinder {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var dir Directories
	if !o.Dir.IsNil() {
		dir = o.Dir
	} else if o.WorkspaceID != uuid.Nil {
		var apperr apperrors.Error
		dir, apperr = getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if apperr != nil {
			return nil
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForVariant(ctx, m.IDS.VariantID)
		if err != nil {
			return nil
		}
	} else {
		return nil
	}

	return func(ctx context.Context, t types.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error) {
		startPath := m.GetStoragePath(t)
		path, obj, err := db.DB(ctx).FindClosestObject(ctx, t, dir.DirForType(t), targetName, startPath)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return "", "", ErrObjectNotFound
			}
			return "", "", ErrCatalogError.Err(err)
		}
		if obj == nil {
			return "", "", ErrObjectNotFound
		}
		hash = obj.Hash
		return
	}
}

func getSchemaLoaderByPath(ctx context.Context, m schemamanager.SchemaMetadata, opts ...ObjectStoreOption) schemamanager.SchemaLoaderByPath {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}
	var dir Directories

	if !o.Dir.IsNil() {
		dir = o.Dir
	} else if o.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if err != nil {
			return nil
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForVariant(ctx, m.IDS.VariantID)
		if err != nil {
			return nil
		}
	} else {
		return nil
	}

	// We do this so load workspace never gets called again
	opts = append(opts, WithDirectories(dir))

	return func(ctx context.Context, t types.CatalogObjectType, m_passed *schemamanager.SchemaMetadata) (schemamanager.SchemaManager, apperrors.Error) {
		return LoadSchemaByPath(ctx, t, m_passed, opts...)
	}
}

func getSchemaLoaderByHash() schemamanager.SchemaLoaderByHash {
	return func(ctx context.Context, t types.CatalogObjectType, hash string, m *schemamanager.SchemaMetadata) (schemamanager.SchemaManager, apperrors.Error) {
		return LoadSchemaByHash(ctx, hash, m)
	}
}

func getSchemaLoaders(ctx context.Context, m schemamanager.SchemaMetadata, opts ...ObjectStoreOption) schemamanager.SchemaLoaders {
	return schemamanager.SchemaLoaders{
		ByPath:        getSchemaLoaderByPath(ctx, m, opts...),
		ByHash:        getSchemaLoaderByHash(),
		ClosestParent: getClosestParentSchemaFinder(ctx, m, opts...),
		SelfMetadata: func() schemamanager.SchemaMetadata {
			return m
		},
	}
}

func getParameterRefForName(refs schemamanager.SchemaReferences) schemamanager.ParameterReferenceForName {
	return func(name string) string {
		for _, ref := range refs {
			if ref.SchemaName() == name {
				return ref.Name
			}
		}
		return ""
	}
}

func getDirectoriesForWorkspace(ctx context.Context, workspaceId uuid.UUID) (Directories, apperrors.Error) {
	var wm schemamanager.WorkspaceManager
	var apperr apperrors.Error
	var dir Directories

	if wm, apperr = LoadWorkspaceManagerByID(ctx, workspaceId); apperr != nil {
		return dir, apperr
	}

	if dir.ParametersDir = wm.ParametersDir(); dir.ParametersDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("workspace does not have a parameters directory")
	}

	if dir.CollectionsDir = wm.CollectionsDir(); dir.CollectionsDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("workspace does not have a collections directory")
	}

	if dir.ValuesDir = wm.ValuesDir(); dir.ValuesDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("workspace does not have a values directory")
	}

	dir.WorkspaceID = workspaceId

	return dir, nil
}

func getDirectoriesForVariant(ctx context.Context, variantId uuid.UUID) (Directories, apperrors.Error) {
	var dir Directories

	v, err := db.DB(ctx).GetVersion(ctx, 1, variantId)
	if err != nil {
		return dir, ErrCatalogError.Err(err)
	}
	if dir.CollectionsDir = v.CollectionsDir; dir.CollectionsDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("variant does not have a collections directory")
	}
	if dir.ParametersDir = v.ParametersDir; dir.ParametersDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("variant does not have a parameters directory")
	}
	if dir.ValuesDir = v.ValuesDir; dir.ValuesDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("variant does not have a values directory")
	}
	// set the variant id
	dir.VariantID = variantId

	return dir, nil
}

func getSchemaReferences(ctx context.Context, t types.CatalogObjectType, dir uuid.UUID, path string) (schemamanager.SchemaReferences, apperrors.Error) {
	var refs schemamanager.SchemaReferences
	r, err := db.DB(ctx).GetAllReferences(ctx, t, dir, path)
	if err != nil {
		return nil, ErrCatalogError.Err(err)
	}
	for _, ref := range r {
		refs = append(refs, schemamanager.SchemaReference{
			Name: ref.Name,
		})
	}
	return refs, nil
}

type objectResource struct {
	name RequestContext
	om   schemamanager.SchemaManager
}

func (or *objectResource) Name() string {
	return or.name.ObjectName
}

func (or *objectResource) Location() string {
	objName := types.ResourceNameFromObjectType(or.name.ObjectType)
	loc := path.Clean("/" + objName + or.om.FullyQualifiedName())
	q := url.Values{}
	if workspace := or.name.WorkspaceLabel; workspace != "" {
		q.Set("workspace", workspace)
	}
	if namespace := or.om.Metadata().Namespace.String(); namespace != "" {
		q.Set("namespace", namespace)
	}
	qStr := q.Encode()
	if qStr != "" {
		loc += "?" + qStr
	}
	return loc
}

func (or *objectResource) Manager() schemamanager.SchemaManager {
	return or.om
}

func (or *objectResource) Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   or.name.Catalog,
		Variant:   types.NullableStringFrom(or.name.Variant),
		Namespace: types.NullableStringFrom(or.name.Namespace),
	}

	object, err := NewSchema(ctx, rsrcJson, m)
	if err != nil {
		return "", err
	}
	err = SaveSchema(ctx, object, WithWorkspaceID(or.name.WorkspaceID), WithErrorIfExists())
	if err != nil {
		return "", err
	}

	or.name.ObjectName = object.Metadata().Name
	or.name.ObjectPath = object.Metadata().Path
	or.name.ObjectType = object.Type()
	or.om = object
	if or.name.Catalog == "" {
		or.name.Catalog = object.Metadata().Catalog
	}
	if or.name.Variant == "" {
		or.name.Variant = object.Metadata().Variant.String()
	}
	if or.name.Namespace == "" {
		or.name.Namespace = object.Metadata().Namespace.String()
	}

	return or.Location(), nil
}

func (or *objectResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   or.name.Catalog,
		Variant:   types.NullableStringFrom(or.name.Variant),
		Namespace: types.NullableStringFrom(or.name.Namespace),
		Path:      or.name.ObjectPath,
		Name:      or.name.ObjectName,
	}
	ves := m.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	m.IDS.CatalogID = or.name.CatalogID
	m.IDS.VariantID = or.name.VariantID

	object, err := LoadSchemaByPath(ctx, or.name.ObjectType, m, WithWorkspaceID(or.name.WorkspaceID))
	if err != nil {
		return nil, err
	}
	return object.ToJson(ctx)
}

func (or *objectResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	if or.name.WorkspaceID == uuid.Nil && or.name.VariantID == uuid.Nil {
		return ErrInvalidWorkspaceOrVariant
	}
	var dir Directories
	var err apperrors.Error
	if or.name.WorkspaceID != uuid.Nil {
		dir, err = getDirectoriesForWorkspace(ctx, or.name.WorkspaceID)
	} else {
		dir, err = getDirectoriesForVariant(ctx, or.name.VariantID)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("workspace_id", or.name.WorkspaceID.String()).Str("variant_id", or.name.VariantID.String()).Msg("failed to get directories")
		return ErrInvalidWorkspaceOrVariant
	}

	m := &schemamanager.SchemaMetadata{
		Catalog:   or.name.Catalog,
		Variant:   types.NullableStringFrom(or.name.Variant),
		Path:      or.name.ObjectPath,
		Name:      or.name.ObjectName,
		Namespace: types.NullableStringFrom(or.name.Namespace),
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	m.IDS.CatalogID = or.name.CatalogID
	m.IDS.VariantID = or.name.VariantID

	// Load the existing object
	existingObj, err := LoadSchemaByPath(ctx, or.name.ObjectType, m, WithDirectories(dir))
	if err != nil {
		return err
	}
	if existingObj == nil {
		return ErrObjectNotFound
	}

	// Create a new object with the updated JSON and save at same path
	newSchema, err := NewSchema(ctx, rsrcJson, m)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create new object")
		return err
	}

	// update the object
	err = SaveSchema(ctx, newSchema, WithDirectories(dir))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object")
		return err
	}

	return nil
}

func (or *objectResource) Delete(ctx context.Context) apperrors.Error {
	if or.name.WorkspaceID == uuid.Nil && or.name.VariantID == uuid.Nil {
		return ErrInvalidWorkspace
	}
	var dir Directories
	var err apperrors.Error
	if or.name.WorkspaceID != uuid.Nil {
		dir, err = getDirectoriesForWorkspace(ctx, or.name.WorkspaceID)
	} else {
		dir, err = getDirectoriesForVariant(ctx, or.name.VariantID)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("workspace_id", or.name.WorkspaceID.String()).Str("variant_id", or.name.VariantID.String()).Msg("failed to get directories")
		return ErrInvalidWorkspace
	}
	m := &schemamanager.SchemaMetadata{
		Catalog:   or.name.Catalog,
		Variant:   types.NullableStringFrom(or.name.Variant),
		Path:      or.name.ObjectPath,
		Name:      or.name.ObjectName,
		Namespace: types.NullableStringFrom(or.name.Namespace),
	}
	pathWithName := path.Clean(m.GetStoragePath(or.name.ObjectType) + "/" + or.name.ObjectName)
	err = DeleteSchema(ctx, or.name.ObjectType, m, dir)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("failed to delete object")
		return err
	}
	return nil
}

func NewSchemaResource(ctx context.Context, name RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if name.Catalog == "" || name.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if name.Variant == "" || name.VariantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	return &objectResource{
		name: name,
	}, nil
}
