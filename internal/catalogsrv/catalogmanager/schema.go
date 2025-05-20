package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	v1Schema "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/v1/schemaresource"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
)

// VersionHeader represents the version and kind information in a schema
type VersionHeader struct {
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

// NewSchema creates a new schema manager from the provided JSON and metadata
func NewSchema(ctx context.Context, rsrcJSON []byte, m *schemamanager.SchemaMetadata) (schemamanager.SchemaManager, apperrors.Error) {
	if len(rsrcJSON) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	var version VersionHeader
	if err := json.Unmarshal(rsrcJSON, &version); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal version header")
		return nil, validationerrors.ErrSchemaValidation
	}

	if version.Version == "" {
		return nil, validationerrors.ErrSchemaValidation.Msg(schemaerr.ErrMissingRequiredAttribute("version").Error())
	}

	if version.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}

	rsrcJSON, m, err := canonicalizeMetadata(rsrcJSON, version.Kind, m)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}

	if err := validateMetadata(ctx, m); err != nil {
		return nil, err
	}

	sm, apperr := v1Schema.NewV1SchemaManager(ctx, rsrcJSON, schemamanager.WithValidation(), schemamanager.WithDefaultValues())
	if apperr != nil {
		return nil, apperr
	}
	sm.SetMetadata(m)

	return sm, nil
}

// storeOptions contains configuration options for storing schemas
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

// Directories represents the directory structure for catalog objects
type Directories struct {
	ParametersDir  uuid.UUID
	CollectionsDir uuid.UUID
	ValuesDir      uuid.UUID
	WorkspaceID    uuid.UUID
	VariantID      uuid.UUID
}

// IsNil checks if all directory IDs are nil
func (d Directories) IsNil() bool {
	return d.ParametersDir == uuid.Nil && d.CollectionsDir == uuid.Nil && d.ValuesDir == uuid.Nil
}

// DirForType returns the appropriate directory ID for the given catalog object type
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

// ObjectStoreOption defines a function that configures store options
type ObjectStoreOption func(*storeOptions)

// WithErrorIfExists returns an option that requires the object to not exist
func WithErrorIfExists() ObjectStoreOption {
	return func(o *storeOptions) {
		o.ErrorIfExists = true
	}
}

// WithErrorIfEqualToExisting returns an option that requires the object to be different from existing
func WithErrorIfEqualToExisting() ObjectStoreOption {
	return func(o *storeOptions) {
		o.ErrorIfEqualToExisting = true
	}
}

// WithWorkspaceID returns an option that sets the workspace ID
func WithWorkspaceID(id uuid.UUID) ObjectStoreOption {
	return func(o *storeOptions) {
		o.WorkspaceID = id
	}
}

// WithDirectories returns an option that sets the directories
func WithDirectories(d Directories) ObjectStoreOption {
	return func(o *storeOptions) {
		o.Dir = d
	}
}

// WithVersionNum returns an option that sets the version number
func WithVersionNum(num int) ObjectStoreOption {
	return func(o *storeOptions) {
		o.VersionNum = num
	}
}

// SkipValidationForUpdate returns an option that skips validation for updates
func SkipValidationForUpdate() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipValidationForUpdate = true
	}
}

// SetDefaultValues returns an option that sets default values
func SetDefaultValues() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SetDefaultValues = true
	}
}

// SkipCanonicalizePaths returns an option that skips path canonicalization
func SkipCanonicalizePaths() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipCanonicalizePaths = true
	}
}

// IgnoreSchemaSpecChange returns an option that ignores schema specification changes
func IgnoreSchemaSpecChange() ObjectStoreOption {
	return func(o *storeOptions) {
		o.IgnoreSchemaSpecChange = true
	}
}

// SkipRevalidationOnSchemaChange returns an option that skips revalidation on schema changes
func SkipRevalidationOnSchemaChange() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipRevalidationOnSchemaChange = true
	}
}

// SaveSchema saves a schema to the database
func SaveSchema(ctx context.Context, om schemamanager.SchemaManager, opts ...ObjectStoreOption) apperrors.Error {
	if om == nil {
		return validationerrors.ErrEmptySchema
	}

	m := om.Metadata()
	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	t := om.Type()
	dir := options.Dir
	rsrcPath := m.GetStoragePath(t)
	pathWithName := path.Clean(rsrcPath + "/" + m.Name)

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
		return ErrInvalidVersionOrWorkspace.Msg("neither workspace ID nor variant ID is provided")
	}

	var (
		existingObjHash string
		refs            schemamanager.SchemaReferences
		existingRefs    schemamanager.SchemaReferences
		existingPath    string
		existingRef     *models.ObjectRef
	)

	switch t {
	case types.CatalogObjectTypeParameterSchema:
		if !options.SkipValidationForUpdate {
			var err apperrors.Error
			existingObjHash, refs, existingPath, existingRef, err = validateParameterSchema(ctx, om, dir, options)
			if err != nil {
				return err
			}
		}
	case types.CatalogObjectTypeCollectionSchema:
		if !options.SkipValidationForUpdate {
			var err apperrors.Error
			existingObjHash, refs, existingRefs, err = validateCollectionSchema(ctx, om, dir, options.ErrorIfExists)
			if err != nil {
				return err
			}
		}
	default:
		return ErrCatalogError.Msg("invalid catalog object type")
	}

	if om.Type() == types.CatalogObjectTypeCollectionSchema {
		om.CollectionSchemaManager().SetDefaultValues(ctx)
	}

	s := om.StorageRepresentation()
	if s == nil {
		return validationerrors.ErrEmptySchema
	}

	hash := s.GetHash()
	if hash == existingObjHash {
		if options.ErrorIfEqualToExisting {
			return ErrAlreadyExists.Msg("parameter schema already exists")
		}
		return nil
	}

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

	if dberr := db.DB(ctx).CreateCatalogObject(ctx, &obj); dberr != nil {
		if !errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
			return ErrCatalogError.Msg("failed to save catalog object")
		}
		log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
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
		return ErrCatalogError.Msg("failed to save object to directory")
	}

	if t == types.CatalogObjectTypeCollectionSchema && !options.SkipValidationForUpdate {
		updateCollectionRefsInParameters(ctx, dir.ParametersDir, pathWithName, existingRefs, refs)
	} else if t == types.CatalogObjectTypeParameterSchema && len(refs) > 0 {
		updateParameterRefsInCollections(ctx, dir, existingPath, pathWithName, existingRef, refs)
	}

	return nil
}

// updateParameterRefsInCollections updates parameter references in collections
func updateParameterRefsInCollections(ctx context.Context, dir Directories, existingPath, newPath string, existingParamObjRef *models.ObjectRef, newCollectionRefs schemamanager.SchemaReferences) {
	var newRefsForExistingParam models.References
	if existingParamObjRef != nil {
		for _, ref := range existingParamObjRef.References {
			remove := false
			for _, newRef := range newCollectionRefs {
				if ref.Name == newRef.Name {
					remove = true
					break
				}
			}
			if !remove {
				newRefsForExistingParam = append(newRefsForExistingParam, ref)
			}
		}
	}

	if len(newRefsForExistingParam) > 0 {
		existingParamObjRef.References = newRefsForExistingParam
		if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, existingPath, *existingParamObjRef); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to update parameter references")
			return
		}
	}

	if existingParamObjRef == nil {
		return
	}

	for _, newRef := range newCollectionRefs {
		if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, newRef.Name, []models.Reference{{Name: newPath}}); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to add new parameter path to collection")
			return
		}
		if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, newRef.Name, existingPath); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to delete parameter path from collection")
			return
		}
	}
}

// updateCollectionRefsInParameters synchronizes collection references in parameters
func updateCollectionRefsInParameters(ctx context.Context, paramDir uuid.UUID, collectionFQP string, existingParamRefs, newParamRefs schemamanager.SchemaReferences) {
	type refAction string
	const (
		actionAdd    refAction = "add"
		actionDelete refAction = "delete"
	)

	refActions := make(map[string]refAction)

	for _, newRef := range newParamRefs {
		refActions[newRef.Name] = actionAdd
	}

	for _, existingRef := range existingParamRefs {
		if _, ok := refActions[existingRef.Name]; !ok {
			refActions[existingRef.Name] = actionDelete
		} else {
			delete(refActions, existingRef.Name)
		}
	}

	for param, action := range refActions {
		switch action {
		case actionAdd:
			if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, []models.Reference{{Name: collectionFQP}}); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collectionschema", collectionFQP).
					Err(err).
					Msg("failed to add references to collection schema")
			}
		case actionDelete:
			if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, collectionFQP); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collectionschema", collectionFQP).
					Err(err).
					Msg("failed to delete references from collection schema")
			}
		}
	}
}

// validateParameterSchema validates a parameter schema
func validateParameterSchema(ctx context.Context, om schemamanager.SchemaManager, dir Directories, options storeOptions) (string, schemamanager.SchemaReferences, string, *models.ObjectRef, apperrors.Error) {
	if om == nil {
		log.Ctx(ctx).Error().Msg("object manager is nil")
		return "", nil, "", nil, ErrCatalogError.Msg("object manager is not initialized")
	}

	pm := om.ParameterSchemaManager()
	if pm == nil {
		log.Ctx(ctx).Error().Msg("parameter manager is nil")
		return "", nil, "", nil, ErrCatalogError.Msg("parameter schema manager is not initialized")
	}

	m := om.Metadata()
	pathWithName := path.Clean(m.GetStoragePath(types.CatalogObjectTypeParameterSchema) + "/" + m.Name)

	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
			err = nil
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			return "", nil, "", nil, ErrCatalogError.Msg("failed to get parameter schema by path")
		}
	}

	var existingObjHash string
	var newRefs schemamanager.SchemaReferences
	var existingPath string
	var existingParamRef *models.ObjectRef

	if r != nil {
		if options.ErrorIfExists {
			return "", nil, "", nil, ErrAlreadyExists.Msg("parameter schema already exists")
		}
		if len(r.References) > 0 {
			sm, err := GetSchemaByHash(ctx, r.Hash, &m)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to load existing parameter schema by hash")
				return "", nil, "", nil, ErrUnableToSaveSchema.Msg("failed to load existing parameter schema")
			}
			pm := sm.ParameterSchemaManager()
			if pm == nil {
				log.Ctx(ctx).Error().Msg("loaded parameter schema manager is nil")
				return "", nil, "", nil, ErrUnableToSaveSchema.Msg("failed to initialize parameter schema manager")
			}
			if !options.IgnoreSchemaSpecChange && om.ParameterSchemaManager().StorageRepresentation().DiffersInSpec(pm.StorageRepresentation()) {
				return "", nil, "", nil, ErrSchemaConflict.Msg("cannot modify parameter schema specification: one or more collection schemas reference this parameter schema")
			}
			for _, ref := range r.References {
				newRefs = append(newRefs, schemamanager.SchemaReference{
					Name: ref.Name,
				})
			}
		}
		existingObjHash = r.Hash
	} else {
		existingPath, existingParamRef, err = db.DB(ctx).FindClosestObject(ctx,
			types.CatalogObjectTypeParameterSchema,
			dir.ParametersDir,
			m.Name,
			m.GetStoragePath(types.CatalogObjectTypeParameterSchema),
		)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("path", existingPath).Msg("failed to find closest object")
			return "", nil, "", nil, ErrCatalogError.Msg("failed to find closest parameter schema")
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
					return "", nil, "", nil, ErrSchemaConflict.Msg("one or more collection schemas in this namespace reference the same parameter schema in root namespace")
				}
			}
		}
	}

	if options.IgnoreSchemaSpecChange {
		if !options.SkipRevalidationOnSchemaChange && len(newRefs) > 0 {
			loaders := getSchemaLoaders(ctx, om.Metadata(), WithDirectories(dir))
			if pm := om.ParameterSchemaManager(); pm != nil {
				if err = pm.ValidateDependencies(ctx, loaders, newRefs); err != nil {
					return "", nil, "", nil, err
				}
			}
		}
	}

	return existingObjHash, newRefs, existingPath, existingParamRef, nil
}

// isParentOrSame checks if p1 is a parent or the same as p2
func isParentOrSame(p1, p2 string) bool {
	p1 = path.Clean(p1)
	p2 = path.Clean(p2)
	return p2 == p1 || strings.HasPrefix(p2, p1+"/")
}

// validateCollectionSchema validates a collection schema
func validateCollectionSchema(ctx context.Context, om schemamanager.SchemaManager, dir Directories, errorIfExists bool) (string, schemamanager.SchemaReferences, schemamanager.SchemaReferences, apperrors.Error) {
	if om == nil {
		log.Ctx(ctx).Error().Msg("object manager is nil")
		return "", nil, nil, ErrCatalogError.Msg("object manager is not initialized")
	}

	cm := om.CollectionSchemaManager()
	if cm == nil {
		log.Ctx(ctx).Error().Msg("collection manager is nil")
		return "", nil, nil, ErrCatalogError.Msg("collection schema manager is not initialized")
	}

	m := om.Metadata()
	parentPath := m.GetStoragePath(types.CatalogObjectTypeCollectionSchema)
	pathWithName := path.Clean(parentPath + "/" + m.Name)

	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			return "", nil, nil, ErrCatalogError.Msg("failed to get collection schema by path")
		}
	}

	var existingObjHash string
	var existingRefs schemamanager.SchemaReferences

	if r != nil {
		if errorIfExists {
			return "", nil, nil, ErrAlreadyExists.Msg("collection schema already exists")
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

	loaders := getSchemaLoaders(ctx, m, WithDirectories(dir), SkipCanonicalizePaths())
	newRefs, err := cm.ValidateDependencies(ctx, loaders, existingRefs)
	if err != nil {
		return "", nil, nil, err
	}

	return existingObjHash, newRefs, existingRefs, nil
}

// deleteCollectionSchema deletes a collection schema
func deleteCollectionSchema(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, dir Directories) apperrors.Error {
	pathWithName := path.Clean(m.GetStoragePath(t) + "/" + m.Name)
	if m.IDS.VariantID == uuid.Nil {
		if err := validateMetadata(ctx, m); err != nil {
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

	hash, err := db.DB(ctx).DeleteObjectWithReferences(ctx,
		types.CatalogObjectTypeCollectionSchema,
		models.DirectoryIDs{
			{ID: dir.CollectionsDir, Type: types.CatalogObjectTypeCollectionSchema},
			{ID: dir.ParametersDir, Type: types.CatalogObjectTypeParameterSchema},
		},
		pathWithName,
		models.DeleteReferences(true),
	)
	if err != nil {
		return ErrCatalogError.Err(err).Msg("unable to delete collection schema from directory")
	}

	if err := db.DB(ctx).DeleteCatalogObject(ctx, types.CatalogObjectTypeCollectionSchema, string(hash)); err != nil {
		if !errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete object from database")
		}
	}
	return nil
}

// deleteParameterSchema deletes a parameter schema
func deleteParameterSchema(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, dir Directories) apperrors.Error {
	pathWithName := path.Clean(m.GetStoragePath(t) + "/" + m.Name)

	refs, err := db.DB(ctx).GetAllReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("failed to get all references")
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Info().Str("path", pathWithName).Msg("no references found, safe to delete parameter schema")
		} else {
			return ErrCatalogError.Err(err).Msg("unable to delete parameter schema")
		}
	} else if len(refs) > 0 {
		log.Ctx(ctx).Info().Str("path", pathWithName).Msg("parameter schema has references, cannot delete")
		return ErrUnableToDeleteParameterWithReferences
	}

	hash, err := db.DB(ctx).DeleteObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		return ErrCatalogError.Err(err).Msg("unable to delete parameter schema from directory")
	}

	if err := db.DB(ctx).DeleteCatalogObject(ctx, types.CatalogObjectTypeParameterSchema, string(hash)); err != nil {
		if !errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete objects from database")
		}
	}
	return nil
}

// collectionSchemaExists checks if a collection schema exists
func collectionSchemaExists(ctx context.Context, collectionsDir uuid.UUID, path string) apperrors.Error {
	if path != "/" {
		exists, err := db.DB(ctx).PathExists(ctx, types.CatalogObjectTypeCollectionSchema, collectionsDir, path)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("path", path).Msg("failed to check if parent path exists")
			return ErrCatalogError
		}
		if !exists {
			return ErrParentCollectionSchemaNotFound.Msg(path + " does not exist")
		}
	}
	return nil
}

var _ = collectionSchemaExists // silence lint

// GetSchema loads a schema by its path
func GetSchema(ctx context.Context, t types.CatalogObjectType, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.SchemaManager, apperrors.Error) {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var dir uuid.UUID
	if !o.Dir.IsNil() && o.Dir.DirForType(t) != uuid.Nil {
		dir = o.Dir.DirForType(t)
	} else if o.WorkspaceID != uuid.Nil {
		dirs, err := getWorkspaceDirs(ctx, o.WorkspaceID)
		if err != nil {
			return nil, err
		}
		dir = dirs.DirForType(t)
	} else if m.IDS.VariantID != uuid.Nil {
		dirs, err := getVariantDirs(ctx, m.IDS.VariantID)
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
	if obj == nil {
		return nil, ErrObjectNotFound
	}

	s := &schemastore.SchemaStorageRepresentation{}
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

// GetSchemaByHash loads a schema by its hash
func GetSchemaByHash(ctx context.Context, hash string, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.SchemaManager, apperrors.Error) {
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

// validateMetadata validates the schema metadata
func validateMetadata(ctx context.Context, m *schemamanager.SchemaMetadata) apperrors.Error {
	if m == nil {
		return ErrEmptyMetadata
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	var catalogID, variantID uuid.UUID
	if c, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, m.Catalog); err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrInvalidCatalog.Err(err)
		}
		return ErrCatalogError.Err(err)
	} else {
		catalogID = c.CatalogID
	}

	if !m.Variant.IsNil() {
		if v, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, m.Variant.String()); err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrVariantNotFound.Err(err)
			}
			return ErrCatalogError.Err(err)
		} else {
			variantID = v.VariantID
		}
	}

	if !m.Namespace.IsNil() {
		if _, err := db.DB(ctx).GetNamespace(ctx, m.Namespace.String(), variantID); err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrNamespaceNotFound.Prefix(m.Namespace.String())
			}
			return ErrCatalogError.Err(err)
		}
	}

	m.IDS.CatalogID = catalogID
	m.IDS.VariantID = variantID
	return nil
}

// getClosestParentSchemaFinder returns a function to find the closest parent schema
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
		dir, apperr = getWorkspaceDirs(ctx, o.WorkspaceID)
		if apperr != nil {
			return nil
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, m.IDS.VariantID)
		if err != nil {
			return nil
		}
	} else {
		return nil
	}

	return func(ctx context.Context, t types.CatalogObjectType, targetName string) (string, string, apperrors.Error) {
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
		return path, obj.Hash, nil
	}
}

// getSchemaLoaderByPath returns a function to load schemas by path
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
		dir, err = getWorkspaceDirs(ctx, o.WorkspaceID)
		if err != nil {
			return nil
		}
	} else if m.IDS.VariantID != uuid.Nil {
		var err apperrors.Error
		dir, err = getVariantDirs(ctx, m.IDS.VariantID)
		if err != nil {
			return nil
		}
	} else {
		return nil
	}

	opts = append(opts, WithDirectories(dir))

	return func(ctx context.Context, t types.CatalogObjectType, m_passed *schemamanager.SchemaMetadata) (schemamanager.SchemaManager, apperrors.Error) {
		return GetSchema(ctx, t, m_passed, opts...)
	}
}

// getSchemaLoaderByHash returns a function to load schemas by hash
func getSchemaLoaderByHash() schemamanager.SchemaLoaderByHash {
	return func(ctx context.Context, t types.CatalogObjectType, hash string, m *schemamanager.SchemaMetadata) (schemamanager.SchemaManager, apperrors.Error) {
		return GetSchemaByHash(ctx, hash, m)
	}
}

// getSchemaLoaders returns a set of schema loaders
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

// getParameterRefForName returns a function to get parameter references by name
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

// getWorkspaceDirs gets the directories for a workspace
func getWorkspaceDirs(ctx context.Context, workspaceID uuid.UUID) (Directories, apperrors.Error) {
	var dir Directories

	wm, err := LoadWorkspaceManagerByID(ctx, workspaceID)
	if err != nil {
		return dir, err
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

	dir.WorkspaceID = workspaceID

	return dir, nil
}

// getVariantDirs gets the directories for a variant
func getVariantDirs(ctx context.Context, variantID uuid.UUID) (Directories, apperrors.Error) {
	var dir Directories

	v, err := db.DB(ctx).GetVersion(ctx, 1, variantID)
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
	dir.VariantID = variantID

	return dir, nil
}

// getSchemaRefs gets the references for a schema
func getSchemaRefs(ctx context.Context, t types.CatalogObjectType, dir uuid.UUID, path string) (schemamanager.SchemaReferences, apperrors.Error) {
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

// objectResource represents a resource object
type objectResource struct {
	name RequestContext
	om   schemamanager.SchemaManager
}

// Name returns the name of the resource
func (or *objectResource) Name() string {
	return or.name.ObjectName
}

// Location returns the location of the resource
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

// Manager returns the schema manager
func (or *objectResource) Manager() schemamanager.SchemaManager {
	return or.om
}

// Create creates a new resource
func (or *objectResource) Create(ctx context.Context, rsrcJSON []byte) (string, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   or.name.Catalog,
		Variant:   types.NullableStringFrom(or.name.Variant),
		Namespace: types.NullableStringFrom(or.name.Namespace),
	}

	object, err := NewSchema(ctx, rsrcJSON, m)
	if err != nil {
		return "", err
	}
	err = SaveSchema(ctx, object, WithWorkspaceID(or.name.WorkspaceID), WithErrorIfExists())
	if err != nil {
		return "", err
	}
	om := object.Metadata()
	or.name.ObjectName = om.Name
	or.name.ObjectPath = om.Path
	or.name.ObjectType = object.Type()
	or.om = object
	if or.name.Catalog == "" {
		or.name.Catalog = om.Catalog
	}
	if or.name.Variant == "" {
		or.name.Variant = om.Variant.String()
	}
	if or.name.Namespace == "" {
		or.name.Namespace = om.Namespace.String()
	}

	return or.Location(), nil
}

// Get gets a resource
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

	object, err := GetSchema(ctx, or.name.ObjectType, m, WithWorkspaceID(or.name.WorkspaceID))
	if err != nil {
		return nil, err
	}
	return object.ToJson(ctx)
}

// Update updates a resource
func (or *objectResource) Update(ctx context.Context, rsrcJSON []byte) apperrors.Error {
	if or.name.WorkspaceID == uuid.Nil && or.name.VariantID == uuid.Nil {
		return ErrInvalidWorkspaceOrVariant
	}
	var dir Directories
	var err apperrors.Error
	if or.name.WorkspaceID != uuid.Nil {
		dir, err = getWorkspaceDirs(ctx, or.name.WorkspaceID)
	} else {
		dir, err = getVariantDirs(ctx, or.name.VariantID)
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

	existingObj, err := GetSchema(ctx, or.name.ObjectType, m, WithDirectories(dir))
	if err != nil {
		return err
	}
	if existingObj == nil {
		return ErrObjectNotFound
	}

	newSchema, err := NewSchema(ctx, rsrcJSON, m)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create new object")
		return err
	}

	err = SaveSchema(ctx, newSchema, WithDirectories(dir))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object")
		return err
	}

	return nil
}

// Delete deletes a resource
func (or *objectResource) Delete(ctx context.Context) apperrors.Error {
	if or.name.WorkspaceID == uuid.Nil && or.name.VariantID == uuid.Nil {
		return ErrInvalidWorkspace
	}
	var dir Directories
	var err apperrors.Error
	if or.name.WorkspaceID != uuid.Nil {
		dir, err = getWorkspaceDirs(ctx, or.name.WorkspaceID)
	} else {
		dir, err = getVariantDirs(ctx, or.name.VariantID)
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

// List returns a list of schemas
func (or *objectResource) List(ctx context.Context) ([]byte, apperrors.Error) {
	return nil, nil
}

// NewSchemaResource creates a new schema resource
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

// DeleteSchema deletes a schema
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
