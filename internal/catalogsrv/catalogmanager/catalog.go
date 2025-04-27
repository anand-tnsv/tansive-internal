package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
)

type catalogSchema struct {
	Version  string          `json:"version" validate:"required,requireVersionV1"`
	Kind     string          `json:"kind" validate:"required,kindValidator"`
	Metadata catalogMetadata `json:"metadata" validate:"required"`
}

type catalogMetadata struct {
	Name        string `json:"name" validate:"required,resourceNameValidator"`
	Description string `json:"description"`
}

type catalogManager struct {
	c models.Catalog
}

var _ schemamanager.CatalogManager = (*catalogManager)(nil)

func (cs *catalogSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	if cs.Kind != types.CatalogKind {
		ves = append(ves, schemaerr.ErrUnsupportedKind("kind"))
	}
	err := schemavalidator.V().Struct(cs)
	if err == nil {
		return ves
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(cs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "resourceNameValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "kindValidator":
			ves = append(ves, schemaerr.ErrUnsupportedKind(jsonFieldName))
		case "requireVersionV1":
			ves = append(ves, schemaerr.ErrInvalidVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	return ves
}

func NewCatalogManager(ctx context.Context, rsrcJson []byte, name string) (schemamanager.CatalogManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(rsrcJson) == 0 {
		return nil, ErrInvalidSchema
	}

	cs := &catalogSchema{}
	if err := json.Unmarshal(rsrcJson, cs); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	ves := cs.Validate()
	if ves != nil {
		return nil, ErrInvalidSchema.Err(ves)
	}

	c := models.Catalog{
		Name:        cs.Metadata.Name,
		Description: cs.Metadata.Description,
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}

	return &catalogManager{
		c: c,
	}, nil
}

func (cm *catalogManager) ID() uuid.UUID {
	return cm.c.CatalogID
}

func (cm *catalogManager) Name() string {
	return cm.c.Name
}

func (cm *catalogManager) Description() string {
	return cm.c.Description
}

func LoadCatalogManagerByName(ctx context.Context, name string) (schemamanager.CatalogManager, apperrors.Error) {
	c, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}
	return &catalogManager{
		c: *c,
	}, nil
}

func (cm *catalogManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateCatalog(ctx, &cm.c)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.New("catalog already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create catalog")
		return err
	}
	return nil
}

func (cm *catalogManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	s := catalogSchema{
		Version: types.VersionV1,
		Kind:    types.CatalogKind,
		Metadata: catalogMetadata{
			Name:        cm.c.Name,
			Description: cm.c.Description,
		},
	}
	j, err := json.Marshal(s)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal json")
		return nil, ErrUnableToLoadObject
	}
	return j, nil
}

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

type catalogResource struct {
	name RequestContext
	cm   schemamanager.CatalogManager
}

func (cr *catalogResource) Name() string {
	return cr.name.Catalog
}

func (cr *catalogResource) Location() string {
	return "/catalogs/" + cr.cm.Name()
}

func (cr *catalogResource) Manager() schemamanager.CatalogManager {
	return cr.cm
}

func (cr *catalogResource) Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error) {
	catalog, err := NewCatalogManager(ctx, rsrcJson, "")
	if err != nil {
		return "", err
	}
	err = catalog.Save(ctx)
	if err != nil {
		return "", err
	}
	cr.cm = catalog
	return cr.Location(), nil
}

func (cr *catalogResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	catalog, err := LoadCatalogManagerByName(ctx, cr.name.Catalog)
	if err != nil {
		return nil, err
	}
	return catalog.ToJson(ctx)
}

func (cr *catalogResource) Delete(ctx context.Context) apperrors.Error {
	err := DeleteCatalogByName(ctx, cr.name.Catalog)
	if err != nil {
		return err
	}
	return nil
}

func (cr *catalogResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	cs := &catalogSchema{}
	if err := json.Unmarshal(rsrcJson, cs); err != nil {
		return ErrInvalidSchema.Err(err)
	}

	ves := cs.Validate()
	if ves != nil {
		return ErrInvalidSchema.Err(ves)
	}

	c, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, cs.Metadata.Name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return err
	}
	c.Description = cs.Metadata.Description

	err = db.DB(ctx).UpdateCatalog(ctx, c)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update catalog")
		return ErrUnableToUpdateObject.Msg("failed to update catalog")
	}
	return nil
}

func NewCatalogResource(ctx context.Context, name RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	return &catalogResource{
		name: name,
	}, nil
}
