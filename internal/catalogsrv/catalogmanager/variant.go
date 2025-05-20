package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
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
	"github.com/tidwall/gjson"
)

type variantSchema struct {
	Version  string          `json:"version" validate:"required"`
	Kind     string          `json:"kind" validate:"required,kindValidator"`
	Metadata variantMetadata `json:"metadata" validate:"required"`
}

type variantMetadata struct {
	Name        string `json:"name" validate:"required,resourceNameValidator"`
	Catalog     string `json:"catalog" validate:"required,resourceNameValidator"`
	Description string `json:"description"`
}

type variantManager struct {
	variant models.Variant
}

var _ schemamanager.VariantManager = (*variantManager)(nil)

func (vs *variantSchema) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	if vs.Kind != types.VariantKind {
		validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind("kind"))
	}

	err := schemavalidator.V().Struct(vs)
	if err == nil {
		return validationErrors
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
		case "resourceNameValidator":
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

func NewVariantManager(ctx context.Context, resourceJSON []byte, name string, catalog string) (schemamanager.VariantManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(resourceJSON) == 0 {
		return nil, ErrInvalidSchema
	}

	vs := &variantSchema{}
	if err := json.Unmarshal(resourceJSON, vs); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}
	if vs.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}
	if vs.Kind != "Variant" {
		return nil, validationerrors.ErrInvalidKind
	}

	// replace name and catalog if not empty
	if name != "" {
		if !schemavalidator.ValidateSchemaName(name) {
			return nil, validationerrors.ErrInvalidNameFormat
		}
		vs.Metadata.Name = name
	}

	if catalog != "" {
		if !schemavalidator.ValidateSchemaName(catalog) {
			return nil, ErrInvalidCatalog
		}
		vs.Metadata.Catalog = catalog
	}

	// validate the schema
	validationErrors := vs.Validate()
	if validationErrors != nil {
		return nil, ErrInvalidSchema.Err(validationErrors)
	}

	// Get catalog ID from context or resolve by name
	catalogID := common.GetCatalogIdFromContext(ctx)
	if catalogID == uuid.Nil {
		var err apperrors.Error
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, vs.Metadata.Catalog)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return nil, ErrCatalogNotFound.New("catalog not found: " + vs.Metadata.Catalog)
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
			return nil, err
		}
	}

	variant := models.Variant{
		Name:        vs.Metadata.Name,
		Description: vs.Metadata.Description,
		CatalogID:   catalogID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}

	return &variantManager{
		variant: variant,
	}, nil
}

func (vm *variantManager) ID() uuid.UUID {
	return vm.variant.VariantID
}

func (vm *variantManager) Name() string {
	return vm.variant.Name
}

func (vm *variantManager) Description() string {
	return vm.variant.Description
}

func (vm *variantManager) CatalogID() uuid.UUID {
	return vm.variant.CatalogID
}

func LoadVariantManager(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) (schemamanager.VariantManager, apperrors.Error) {
	if variantID == uuid.Nil && (catalogID == uuid.Nil || name == "") {
		return nil, ErrInvalidVariant.Msg("variant ID or both catalog ID and name must be provided")
	}
	variant, err := db.DB(ctx).GetVariant(ctx, catalogID, variantID, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
		return nil, err
	}
	return &variantManager{
		variant: *variant,
	}, nil
}

func (vm *variantManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateVariant(ctx, &vm.variant)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.Msg("variant already exists")
		}
		if errors.Is(err, dberror.ErrInvalidCatalog) {
			return ErrInvalidVariant.Msg("catalog does not exist or is invalid")
		}
		if errors.Is(err, dberror.ErrInvalidInput) {
			return ErrInvalidVariant.Msg("invalid variant name format")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create variant")
		return err
	}
	return nil
}

func (vm *variantManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	// Get name of the catalog
	catalog, err := db.DB(ctx).GetCatalog(ctx, vm.variant.CatalogID, "")
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}

	s := variantSchema{
		Version: types.VersionV1,
		Kind:    types.VariantKind,
		Metadata: variantMetadata{
			Name:        vm.variant.Name,
			Catalog:     catalog.Name,
			Description: vm.variant.Description,
		},
	}

	j, e := json.Marshal(s)
	if e != nil {
		log.Ctx(ctx).Error().Err(e).Msg("failed to marshal json")
		return nil, ErrUnableToLoadObject
	}

	return j, nil
}

func DeleteVariant(ctx context.Context, catalogID, variantID uuid.UUID, name string) apperrors.Error {
	err := db.DB(ctx).DeleteVariant(ctx, catalogID, variantID, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete catalog")
		return err
	}
	return nil
}

// TODO Handle base variant and copy of data

type variantResource struct {
	name RequestContext
	vm   schemamanager.VariantManager
}

func (vr *variantResource) Name() string {
	return vr.name.Variant
}

func (vr *variantResource) Location() string {
	return "/variants/" + vr.vm.ID().String()
}

func (vr *variantResource) Manager() schemamanager.VariantManager {
	return vr.vm
}

func (vr *variantResource) Create(ctx context.Context, resourceJSON []byte) (string, apperrors.Error) {
	vm, err := NewVariantManager(ctx, resourceJSON, vr.name.Variant, vr.name.Catalog)
	if err != nil {
		return "", err
	}
	if err := vm.Save(ctx); err != nil {
		return "", err
	}
	vr.name.Variant = vm.Name()
	vr.name.VariantID = vm.ID()
	vr.name.CatalogID = vm.CatalogID()
	vr.vm = vm
	vr.name.Catalog = gjson.GetBytes(resourceJSON, "metadata.catalog").String()
	return vr.Location(), nil
}

func (vr *variantResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	variant, err := LoadVariantManager(ctx, vr.name.CatalogID, vr.name.VariantID, vr.name.Variant)
	if err != nil {
		return nil, err
	}
	return variant.ToJson(ctx)
}

func (vr *variantResource) Delete(ctx context.Context) apperrors.Error {
	return DeleteVariant(ctx, vr.name.CatalogID, vr.name.VariantID, vr.name.Variant)
}

func (vr *variantResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	vs := &variantSchema{}
	if err := json.Unmarshal(rsrcJson, vs); err != nil {
		return ErrInvalidSchema.Err(err)
	}

	ves := vs.Validate()
	if ves != nil {
		return ErrInvalidSchema.Err(ves)
	}

	v, err := db.DB(ctx).GetVariant(ctx, vr.name.CatalogID, vr.name.VariantID, vs.Metadata.Name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return err
	}
	v.Description = vs.Metadata.Description

	err = db.DB(ctx).UpdateVariant(ctx, uuid.Nil, vr.name.Variant, v)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update variant")
		return ErrUnableToUpdateObject.Msg("failed to update variant")
	}

	return nil
}

func (vr *variantResource) List(ctx context.Context) ([]byte, apperrors.Error) {
	return nil, nil
}

func NewVariantResource(ctx context.Context, name RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if name.Catalog == "" || name.CatalogID == uuid.Nil {
		return nil, ErrInvalidVariant.Msg("catalog name and ID are required for variant creation")
	}
	return &variantResource{
		name: name,
	}, nil
}
