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
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(vs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(vs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
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

func GetValue(ctx context.Context, m *ValueMetadata, dir Directories) (*valueSchema, apperrors.Error) {
	// load the object manager
	om, err := GetSchema(ctx,
		types.CatalogObjectTypeCollectionSchema,
		&schemamanager.SchemaMetadata{
			Catalog: m.Catalog,
			Variant: m.Variant,
			Path:    path.Dir(m.Collection),
			Name:    path.Base(m.Collection),
		},
		WithDirectories(dir))

	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object manager")
		if errors.Is(err, ErrObjectNotFound) {
			return nil, ErrInvalidCollection.Msg("invalid collection " + m.Collection)
		}
		return nil, ErrCatalogError.Err(err)
	}

	// get the values
	valuesParam := om.CollectionSchemaManager().GetDefaultValues()
	values := make(valueSpec)
	for param, value := range valuesParam {
		values[param] = value.Value
	}

	vs := &valueSchema{
		Version: om.Version(),
		Kind:    om.Kind(),
		Metadata: ValueMetadata{
			Catalog:    om.Metadata().Catalog,
			Variant:    om.Metadata().Variant,
			Collection: om.FullyQualifiedName(),
		},
		Spec: values,
	}

	return vs, nil
}

func SaveValue(ctx context.Context, valueJson []byte, m *ValueMetadata, opts ...ObjectStoreOption) apperrors.Error {
	if len(valueJson) == 0 {
		return validationerrors.ErrEmptySchema
	}

	// get the options
	options := &storeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	v := valueSchema{}
	if err := json.Unmarshal(valueJson, &v); err != nil {
		log.Ctx(ctx).Debug().Err(err).Msg("failed to unmarshal value schema")
		return validationerrors.ErrInvalidSchema
	}

	if err := canonicalizeValueMetadata(v, m); err != nil {
		return err
	}

	if err := v.Validate(); err != nil {
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
	om, err := GetSchema(ctx,
		types.CatalogObjectTypeCollectionSchema,
		&schemamanager.SchemaMetadata{
			Catalog: v.Metadata.Catalog,
			Variant: v.Metadata.Variant,
			Path:    path.Dir(v.Metadata.Collection),
			Name:    path.Base(v.Metadata.Collection),
		},
		WithDirectories(dir))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object manager")
		if errors.Is(err, ErrObjectNotFound) {
			return ErrInvalidCollection.Msg("invalid collection " + v.Metadata.Collection)
		}
		return ErrCatalogError.Err(err)
	}

	oldHash := om.StorageRepresentation().GetHash()

	// get object References
	refs, err := getSchemaRefs(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, v.Metadata.Collection)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to get object references")
		refs = schemamanager.SchemaReferences{}
	}

	// get the loaders
	loaders := getSchemaLoaders(ctx, om.Metadata(), WithDirectories(dir))
	loaders.ParameterRef = getParameterRefForName(refs)

	// validate the value against the collection
	c := om.CollectionSchemaManager()
	if c == nil {
		return validationerrors.ErrSchemaValidation.Msg("failed to load collection manager")
	}
	for param, value := range v.Spec {
		v := c.GetValue(ctx, param)
		if v.Value.Equals(value) {
			continue
		}
		if err := c.ValidateValue(ctx, loaders, param, value); err != nil {
			return err
		}
		c.SetValue(ctx, param, value)
	}

	s := c.StorageRepresentation()
	hash := s.GetHash()

	if hash == oldHash {
		return nil
	}

	// save the collection object
	data, e := s.Serialize()
	if e != nil {
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

	if err := db.DB(ctx).AddOrUpdateObjectByPath(
		ctx, types.CatalogObjectTypeCollectionSchema,
		dir.DirForType(types.CatalogObjectTypeCollectionSchema),
		v.Metadata.Collection,
		models.ObjectRef{
			Hash:       obj.Hash,
			References: refModel,
		}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
		return ErrCatalogError
	}

	return nil
}

func canonicalizeValueMetadata(v valueSchema, m *ValueMetadata) apperrors.Error {
	if m != nil {
		if m.Catalog != "" {
			v.Metadata.Catalog = m.Catalog
		}
		if !m.Variant.IsNil() {
			v.Metadata.Variant = m.Variant
		}
		if m.Collection != "" {
			v.Metadata.Collection = m.Collection
		}
	}

	if v.Metadata.Variant.IsNil() {
		v.Metadata.Variant = types.NullableString{Value: types.DefaultVariant, Valid: true}
	}

	return nil
}
