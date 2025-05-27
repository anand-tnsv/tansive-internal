package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type valueSchema struct {
	Version  string        `json:"version" validate:"required"`
	Kind     string        `json:"kind" validate:"required,kindValidator"`
	Metadata ValueMetadata `json:"metadata" validate:"required"`
	Spec     valueSpec     `json:"spec" validate:"required"`
}

type ValueMetadata struct {
	Catalog    string               `json:"catalog" validate:"required,resourceNameValidator"`
	Variant    types.NullableString `json:"variant" validate:"required,resourceNameValidator"`
	Collection string               `json:"collection" validate:"required,resourcePathValidator"`
}

type valueSpec map[string]types.NullableAny

func (vs *valueSchema) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(vs)
	if err == nil {
		return nil
	}
	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(validationErrors, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(vs).Elem()
	typeOfCS := value.Type()

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		default:
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return validationErrors
}

func GetValue(ctx context.Context, metadata *ValueMetadata, dir Directories) (*valueSchema, apperrors.Error) {
	// load the object manager
	objectManager, err := GetSchema(ctx,
		catcommon.CatalogObjectTypeCollectionSchema,
		&schemamanager.SchemaMetadata{
			Catalog: metadata.Catalog,
			Variant: metadata.Variant,
			Path:    path.Dir(metadata.Collection),
			Name:    path.Base(metadata.Collection),
		},
		WithDirectories(dir))

	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object manager")
		if errors.Is(err, ErrObjectNotFound) {
			return nil, ErrInvalidCollection.Msg("invalid collection " + metadata.Collection)
		}
		return nil, ErrCatalogError.Err(err)
	}

	// get the values
	defaultValues := objectManager.CollectionSchemaManager().GetDefaultValues()
	values := make(valueSpec)
	for param, value := range defaultValues {
		values[param] = value.Value
	}

	valueSchema := &valueSchema{
		Version: objectManager.Version(),
		Kind:    objectManager.Kind(),
		Metadata: ValueMetadata{
			Catalog:    objectManager.Metadata().Catalog,
			Variant:    objectManager.Metadata().Variant,
			Collection: objectManager.FullyQualifiedName(),
		},
		Spec: values,
	}

	return valueSchema, nil
}

func SaveValue(ctx context.Context, valueJSON []byte, metadata *ValueMetadata, opts ...ObjectStoreOption) apperrors.Error {
	if len(valueJSON) == 0 {
		return validationerrors.ErrEmptySchema
	}

	// get the options
	options := &storeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	valueSchema := valueSchema{}
	if err := json.Unmarshal(valueJSON, &valueSchema); err != nil {
		log.Ctx(ctx).Debug().Err(err).Msg("failed to unmarshal value schema")
		return validationerrors.ErrInvalidSchema
	}

	if err := canonicalizeValueMetadata(valueSchema, metadata); err != nil {
		return err
	}

	if err := valueSchema.Validate(); err != nil {
		return validationerrors.ErrSchemaValidation.Msg(err.Error())
	}

	var dir Directories

	// get the directories
	if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getWorkspaceDirs(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	// load the object manager
	objectManager, err := GetSchema(ctx,
		catcommon.CatalogObjectTypeCollectionSchema,
		&schemamanager.SchemaMetadata{
			Catalog: valueSchema.Metadata.Catalog,
			Variant: valueSchema.Metadata.Variant,
			Path:    path.Dir(valueSchema.Metadata.Collection),
			Name:    path.Base(valueSchema.Metadata.Collection),
		},
		WithDirectories(dir))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object manager")
		if errors.Is(err, ErrObjectNotFound) {
			return ErrInvalidCollection.Msg("invalid collection " + valueSchema.Metadata.Collection)
		}
		return ErrCatalogError.Err(err)
	}

	oldHash := objectManager.StorageRepresentation().GetHash()

	// get object References
	refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, valueSchema.Metadata.Collection)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to get object references")
		refs = schemamanager.SchemaReferences{}
	}

	// get the loaders
	loaders := getSchemaLoaders(ctx, objectManager.Metadata(), WithDirectories(dir))
	loaders.ParameterRef = getParameterRefForName(refs)

	// validate the value against the collection
	collectionManager := objectManager.CollectionSchemaManager()
	if collectionManager == nil {
		return validationerrors.ErrSchemaValidation.Msg("failed to load collection manager")
	}
	for param, value := range valueSchema.Spec {
		paramValue := collectionManager.GetValue(ctx, param)
		if paramValue.Value.Equals(value) {
			continue
		}
		if err := collectionManager.ValidateValue(ctx, loaders, param, value); err != nil {
			return err
		}
		collectionManager.SetValue(ctx, param, value)
	}

	storageRep := collectionManager.StorageRepresentation()
	hash := storageRep.GetHash()

	if hash == oldHash {
		return nil
	}

	// save the collection object
	data, err := storageRep.Serialize()
	if err != nil {
		return validationerrors.ErrSchemaSerialization
	}
	catalogObject := models.CatalogObject{
		Type:    storageRep.Type,
		Version: storageRep.Version,
		Data:    data,
		Hash:    hash,
	}
	// Save catalogObject to the database
	dbErr := db.DB(ctx).CreateCatalogObject(ctx, &catalogObject)
	if dbErr != nil {
		if errors.Is(dbErr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", catalogObject.Hash).Msg("catalog object already exists")
			// in this case, we don't return. If we came here it means the object is not in the directory,
			// so we'll keep chugging along and save the object to the directory
		} else {
			log.Ctx(ctx).Error().Err(dbErr).Msg("failed to save catalog object")
			return dbErr
		}
	}
	var refModel models.References
	for _, ref := range refs {
		refModel = append(refModel, models.Reference{
			Name: ref.Name,
		})
	}

	if err := db.DB(ctx).AddOrUpdateObjectByPath(
		ctx, catcommon.CatalogObjectTypeCollectionSchema,
		dir.DirForType(catcommon.CatalogObjectTypeCollectionSchema),
		valueSchema.Metadata.Collection,
		models.ObjectRef{
			Hash:       catalogObject.Hash,
			References: refModel,
		}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
		return ErrCatalogError
	}

	return nil
}

func canonicalizeValueMetadata(valueSchema valueSchema, metadata *ValueMetadata) apperrors.Error {
	if metadata != nil {
		if metadata.Catalog != "" {
			valueSchema.Metadata.Catalog = metadata.Catalog
		}
		if !metadata.Variant.IsNil() {
			valueSchema.Metadata.Variant = metadata.Variant
		}
		if metadata.Collection != "" {
			valueSchema.Metadata.Collection = metadata.Collection
		}
	}
	return nil
}
