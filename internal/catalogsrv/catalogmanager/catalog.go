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
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

// catalogSchema represents the structure of a catalog definition
type catalogSchema struct {
	Version  string          `json:"version" validate:"required,requireVersionV1"`
	Kind     string          `json:"kind" validate:"required,kindValidator"`
	Metadata catalogMetadata `json:"metadata" validate:"required"`
}

// catalogMetadata contains metadata about a catalog
type catalogMetadata struct {
	Name        string `json:"name" validate:"required,resourceNameValidator"`
	Description string `json:"description"`
}

// catalogManager implements the schemamanager.CatalogManager interface
type catalogManager struct {
	catalog models.Catalog
}

var _ schemamanager.CatalogManager = (*catalogManager)(nil)

// Validate performs validation on the catalog schema
func (cs *catalogSchema) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	if cs.Kind != types.CatalogKind {
		validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind("kind"))
	}

	err := schemavalidator.V().Struct(cs)
	if err == nil {
		return validationErrors
	}

	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(validationErrors, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(cs).Elem()
	typeOfSchema := value.Type()

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfSchema, e.StructField())

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

// NewCatalogManager creates a new catalog manager from JSON input
func NewCatalogManager(ctx context.Context, resourceJSON []byte, name string) (schemamanager.CatalogManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(resourceJSON) == 0 {
		return nil, ErrInvalidSchema
	}

	schema := &catalogSchema{}
	if err := json.Unmarshal(resourceJSON, schema); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	validationErrors := schema.Validate()
	if validationErrors != nil {
		return nil, ErrInvalidSchema.Err(validationErrors)
	}

	catalog := models.Catalog{
		Name:        schema.Metadata.Name,
		Description: schema.Metadata.Description,
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}

	return &catalogManager{
		catalog: catalog,
	}, nil
}

// ID returns the catalog's UUID
func (cm *catalogManager) ID() uuid.UUID {
	return cm.catalog.CatalogID
}

// Name returns the catalog's name
func (cm *catalogManager) Name() string {
	return cm.catalog.Name
}

// Description returns the catalog's description
func (cm *catalogManager) Description() string {
	return cm.catalog.Description
}

// LoadCatalogManagerByName loads a catalog manager by its name
func LoadCatalogManagerByName(ctx context.Context, name string) (schemamanager.CatalogManager, apperrors.Error) {
	catalog, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}
	return &catalogManager{
		catalog: *catalog,
	}, nil
}

// Save persists the catalog to the database
func (cm *catalogManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateCatalog(ctx, &cm.catalog)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.New("catalog already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create catalog")
		return err
	}
	return nil
}

// ToJson converts the catalog to its JSON representation
func (cm *catalogManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	schema := catalogSchema{
		Version: types.VersionV1,
		Kind:    types.CatalogKind,
		Metadata: catalogMetadata{
			Name:        cm.catalog.Name,
			Description: cm.catalog.Description,
		},
	}

	jsonData, err := json.Marshal(schema)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal catalog to JSON")
		return nil, ErrUnableToLoadObject
	}
	return jsonData, nil
}

// DeleteCatalogByName deletes a catalog by its name
func DeleteCatalogByName(ctx context.Context, name string) apperrors.Error {
	err := db.DB(ctx).DeleteCatalog(ctx, uuid.Nil, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete catalog")
		return err
	}
	return nil
}

// catalogKind implements the ResourceManager interface for catalogs
type catalogKind struct {
	req     RequestContext
	manager schemamanager.CatalogManager
}

// Name returns the catalog name
func (c *catalogKind) Name() string {
	return c.req.Catalog
}

// Location returns the resource location
func (c *catalogKind) Location() string {
	return "/catalogs/" + c.manager.Name()
}

// Manager returns the catalog manager
func (c *catalogKind) Manager() schemamanager.CatalogManager {
	return c.manager
}

// Create creates a new catalog
func (c *catalogKind) Create(ctx context.Context, resourceJSON []byte) (string, apperrors.Error) {
	catalog, err := NewCatalogManager(ctx, resourceJSON, "")
	if err != nil {
		return "", err
	}

	err = catalog.Save(ctx)
	if err != nil {
		return "", err
	}

	c.manager = catalog
	return c.Location(), nil
}

// Get retrieves a catalog
func (c *catalogKind) Get(ctx context.Context) ([]byte, apperrors.Error) {
	catalog, err := LoadCatalogManagerByName(ctx, c.req.Catalog)
	if err != nil {
		return nil, err
	}
	return catalog.ToJson(ctx)
}

// Delete removes a catalog
func (c *catalogKind) Delete(ctx context.Context) apperrors.Error {
	return DeleteCatalogByName(ctx, c.req.Catalog)
}

// Update modifies an existing catalog
func (c *catalogKind) Update(ctx context.Context, resourceJSON []byte) apperrors.Error {
	schema := &catalogSchema{}
	if err := json.Unmarshal(resourceJSON, schema); err != nil {
		return ErrInvalidSchema.Err(err)
	}

	validationErrors := schema.Validate()
	if validationErrors != nil {
		return ErrInvalidSchema.Err(validationErrors)
	}

	catalog, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, schema.Metadata.Name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return err
	}

	catalog.Description = schema.Metadata.Description

	err = db.DB(ctx).UpdateCatalog(ctx, catalog)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update catalog")
		return ErrUnableToUpdateObject.Msg("failed to update catalog")
	}
	return nil
}

// List returns a list of catalogs
func (c *catalogKind) List(ctx context.Context) ([]byte, apperrors.Error) {
	return nil, nil
}

// NewCatalogKindHandler creates a new catalog resource
func NewCatalogKindHandler(ctx context.Context, requestContext RequestContext) (schemamanager.KindHandler, apperrors.Error) {
	return &catalogKind{
		req: requestContext,
	}, nil
}
