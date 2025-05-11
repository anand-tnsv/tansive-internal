package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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
)

type namespaceSchema struct {
	Version  string            `json:"version" validate:"requireVersionV1"`
	Kind     string            `json:"kind" validate:"required,kindValidator"`
	Metadata namespaceMetadata `json:"metadata" validate:"required"`
}

type namespaceMetadata struct {
	Catalog     string `json:"catalog" validate:"omitempty,resourceNameValidator"`
	Variant     string `json:"variant" validate:"omitempty,resourceNameValidator"`
	Name        string `json:"name" validate:"required,resourceNameValidator"`
	Description string `json:"description"`
}

type namespaceManager struct {
	namespace models.Namespace
}

// var _ schemamanager.VariantManager = (*variantManager)(nil)

func NewNamespaceManager(ctx context.Context, resourceJSON []byte, catalog string, variant string) (schemamanager.NamespaceManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(resourceJSON) == 0 {
		return nil, ErrInvalidSchema
	}

	ns := &namespaceSchema{}
	if err := json.Unmarshal(resourceJSON, ns); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	validationErrors := ns.Validate()
	if validationErrors != nil {
		return nil, ErrInvalidSchema.Err(validationErrors)
	}

	if catalog != "" {
		if !schemavalidator.ValidateSchemaName(catalog) {
			return nil, ErrInvalidCatalog
		}
		ns.Metadata.Catalog = catalog
	}

	if variant != "" {
		if !schemavalidator.ValidateSchemaName(variant) {
			return nil, validationerrors.ErrInvalidNameFormat
		}
		ns.Metadata.Variant = variant
	}

	catalogID := common.GetCatalogIdFromContext(ctx)
	variantID := common.GetVariantIdFromContext(ctx)

	if catalogID == uuid.Nil || ns.Metadata.Catalog != common.GetCatalogFromContext(ctx) {
		var err apperrors.Error
		// retrieve the catalogID
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, ns.Metadata.Catalog)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return nil, ErrCatalogNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
			return nil, err
		}
	}

	// retrieve the variantID
	if variantID == uuid.Nil || ns.Metadata.Variant != common.GetVariantFromContext(ctx) {
		var err apperrors.Error
		variantID, err = db.DB(ctx).GetVariantIDFromName(ctx, catalogID, ns.Metadata.Variant)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return nil, ErrVariantNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
			return nil, err
		}
	}

	namespace := models.Namespace{
		Description: ns.Metadata.Description,
		VariantID:   variantID,
		CatalogID:   catalogID,
		Name:        ns.Metadata.Name,
		Catalog:     ns.Metadata.Catalog,
		Variant:     ns.Metadata.Variant,
		Info:        nil,
	}

	return &namespaceManager{
		namespace: namespace,
	}, nil
}

func (ns *namespaceSchema) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	if ns.Kind != types.NamespaceKind {
		validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind("kind"))
	}

	err := schemavalidator.V().Struct(ns)
	if err == nil {
		return validationErrors
	}

	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(validationErrors, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(ns).Elem()
	typeOfCS := value.Type()

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "kindValidator":
			validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind(jsonFieldName))
		case "requireVersionV1":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidVersion(jsonFieldName))
		default:
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return validationErrors
}

func (nm *namespaceManager) Name() string {
	return nm.namespace.Name
}

func (nm *namespaceManager) Description() string {
	return nm.namespace.Description
}

func (nm *namespaceManager) VariantID() uuid.UUID {
	return nm.namespace.VariantID
}

func (nm *namespaceManager) CatalogID() uuid.UUID {
	return nm.namespace.CatalogID
}

func (nm *namespaceManager) Catalog() string {
	return nm.namespace.Catalog
}

func (nm *namespaceManager) Variant() string {
	return nm.namespace.Variant
}

func (nm *namespaceManager) GetNamespaceModel() *models.Namespace {
	return &nm.namespace
}

func LoadNamespaceManagerByName(ctx context.Context, variantID uuid.UUID, name string) (schemamanager.NamespaceManager, apperrors.Error) {
	if variantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	namespace, err := db.DB(ctx).GetNamespace(ctx, name, variantID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load namespace")
		return nil, ErrCatalogError.Msg("unable to load namespace")
	}
	return &namespaceManager{
		namespace: *namespace,
	}, nil
}

func (nm *namespaceManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateNamespace(ctx, &nm.namespace)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.Msg("namespace already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create namespace")
		return ErrCatalogError.Msg("unable to create namespace")
	}
	return nil
}

func (nm *namespaceManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	ns := &namespaceSchema{
		Version: "v1",
		Kind:    types.NamespaceKind,
		Metadata: namespaceMetadata{
			Catalog:     nm.namespace.Catalog,
			Variant:     nm.namespace.Variant,
			Name:        nm.namespace.Name,
			Description: nm.namespace.Description,
		},
	}

	jsonData, e := json.Marshal(ns)
	if e != nil {
		log.Ctx(ctx).Error().Err(e).Msg("unable to marshal workspace schema")
		return nil, ErrCatalogError.Msg("unable to marshal workspace schema")
	}

	return jsonData, nil
}

func DeleteNamespace(ctx context.Context, name string, variantID uuid.UUID, dir Directories) apperrors.Error {
	// check if the namespace exists by retrieving it
	_, err := db.DB(ctx).GetNamespace(ctx, name, variantID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace")
		return err
	}

	// delete the namespace
	err = db.DB(ctx).DeleteNamespace(ctx, name, variantID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace")
		return err
	}

	// delete the namespace objects in all directories
	_, err = db.DB(ctx).DeleteNamespaceObjects(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, name)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace objects in ParameterSchema")
	}
	_, err = db.DB(ctx).DeleteNamespaceObjects(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, name)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace objects in CollectionSchema")
	}
	_, err = db.DB(ctx).DeleteNamespaceObjects(ctx, types.CatalogObjectTypeCatalogCollection, dir.ValuesDir, name)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace objects in CatalogCollection")
	}

	return nil
}

type namespaceResource struct {
	name RequestContext
	nm   schemamanager.NamespaceManager
}

func (nr *namespaceResource) Name() string {
	return nr.name.Namespace
}

func (nr *namespaceResource) ID() uuid.UUID {
	return nr.name.VariantID
}

func (nr *namespaceResource) Location() string {
	return "/namespaces/" + nr.name.Namespace
}

func (nr *namespaceResource) Manager() schemamanager.NamespaceManager {
	return nr.nm
}

func (nr *namespaceResource) Create(ctx context.Context, resourceJSON []byte) (string, apperrors.Error) {
	nm, err := NewNamespaceManager(ctx, resourceJSON, nr.name.Catalog, nr.name.Variant)
	if err != nil {
		return "", err
	}
	if err := nm.Save(ctx); err != nil {
		return "", err
	}
	nr.name.Namespace = nm.Name()
	if nr.name.Catalog == "" {
		nr.name.Catalog = nm.Catalog()
	}
	if nr.name.Variant == "" {
		nr.name.Variant = nm.Variant()
	}
	return nr.Location(), nil
}

func (nr *namespaceResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	if nr.name.VariantID == uuid.Nil || nr.name.Namespace == "" {
		return nil, ErrInvalidNamespace
	}
	namespace, err := LoadNamespaceManagerByName(ctx, nr.name.VariantID, nr.name.Namespace)
	if err != nil {
		if errors.Is(err, ErrNamespaceNotFound) {
			return nil, ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load namespace")
		return nil, ErrUnableToLoadObject.Msg("unable to load namespace")
	}
	jsonData, err := namespace.ToJson(ctx)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to marshal namespace schema")
		return nil, ErrUnableToLoadObject.Msg("unable to marshal namespace schema")
	}
	nr.nm = namespace
	return jsonData, nil
}

func (nr *namespaceResource) Delete(ctx context.Context) apperrors.Error {
	// delete the namespace
	var dir Directories
	var err apperrors.Error
	if nr.name.WorkspaceID != uuid.Nil {
		dir, err = getWorkspaceDirs(ctx, nr.name.WorkspaceID)
	} else {
		dir, err = getVariantDirs(ctx, nr.name.VariantID)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to get directories")
	}
	return DeleteNamespace(ctx, nr.name.Namespace, nr.name.VariantID, dir)
}

func (nr *namespaceResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	ns := &namespaceSchema{}
	if err := json.Unmarshal(rsrcJson, ns); err != nil {
		return ErrInvalidSchema.Err(err)
	}
	ves := ns.Validate()
	if ves != nil {
		return ErrInvalidSchema.Err(ves)
	}
	_, err := nr.Get(ctx)
	if err != nil {
		return err
	}
	namespace := nr.nm.GetNamespaceModel()
	if namespace == nil {
		return ErrInvalidNamespace
	}
	namespace.Description = ns.Metadata.Description
	namespace.Name = ns.Metadata.Name
	err = db.DB(ctx).UpdateNamespace(ctx, namespace)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update namespace")
		return ErrUnableToLoadObject.Msg("unable to update namespace")
	}
	nr.name.Namespace = namespace.Name
	return nil
}

func NewNamespaceResource(ctx context.Context, name RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if name.Catalog == "" || name.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if name.Variant == "" || name.VariantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	return &namespaceResource{
		name: name,
	}, nil
}
