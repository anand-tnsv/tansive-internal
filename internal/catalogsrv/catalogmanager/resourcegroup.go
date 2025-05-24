package catalogmanager

import (
	"context"
	"path"

	"github.com/google/uuid"
	json "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/objectstore"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

// NewResourceGroupManager creates a new ResourceGroupManager instance from the provided JSON schema and metadata.
// It validates the schema and metadata before creating the manager.
func NewResourceGroupManager(ctx context.Context, rsrcJson []byte, m *schemamanager.SchemaMetadata) (schemamanager.ResourceGroupManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	// Get the metadata, replace fields in JSON from provided metadata, and set defaults.
	rsrcJson, m, err := canonicalizeMetadata(rsrcJson, types.ResourceGroupKind, m)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to canonicalize metadata")
		return nil, validationerrors.ErrSchemaSerialization
	}

	var rg ResourceGroup
	if err := json.Unmarshal(rsrcJson, &rg); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to unmarshal resource group")
		return nil, validationerrors.ErrSchemaValidation
	}

	if ves := rg.Validate(); ves != nil {
		log.Ctx(ctx).Error().Err(ves).Msg("Resource group validation failed")
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	rg.Metadata = *m

	return &resourceGroupManager{resourceGroup: rg}, nil
}

func LoadResourceGroupManagerByPath(ctx context.Context, m *schemamanager.SchemaMetadata) (schemamanager.ResourceGroupManager, apperrors.Error) {
	if m == nil {
		return nil, ErrInvalidObject.Msg("unable to infer object metadata")
	}

	// Get the directory ID for the resource group
	catalogID := common.GetCatalogIdFromContext(ctx)
	var err apperrors.Error

	if catalogID == uuid.Nil {
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, m.Catalog)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("catalog", m.Catalog).Msg("Failed to get catalog ID by name")
			return nil, err
		}
	}

	v, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, m.Variant.String())
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("catalogID", catalogID.String()).Str("name", m.Name).Msg("Failed to get variant")
		return nil, err
	}

	pathWithName := path.Clean(m.GetStoragePath(types.CatalogObjectTypeResourceGroup) + "/" + m.Name)

	rg, err := db.DB(ctx).GetResourceGroupObject(ctx, pathWithName, v.ResourceGroupsDirectoryID)
	if err != nil {
		return nil, err
	}

	return resourceGroupManagerFromObject(ctx, rg, m)
}

func resourceGroupManagerFromObject(ctx context.Context, obj *models.CatalogObject, m *schemamanager.SchemaMetadata) (schemamanager.ResourceGroupManager, apperrors.Error) {
	if obj == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	s := objectstore.ObjectStorageRepresentation{}
	if err := json.Unmarshal(obj.Data, &s); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource group")
		return nil, validationerrors.ErrSchemaValidation
	}
	if s.Type != types.CatalogObjectTypeResourceGroup {
		log.Ctx(ctx).Error().Msg("invalid type")
		return nil, ErrUnableToLoadObject
	}

	rgm := &resourceGroupManager{}
	if err := json.Unmarshal(s.Spec, &rgm.resourceGroup.Spec); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource group schema spec")
		return nil, ErrUnableToLoadObject
	}
	rgm.resourceGroup.Kind = types.ResourceGroupKind
	rgm.resourceGroup.Version = s.Version
	rgm.resourceGroup.Metadata = *m

	return rgm, nil
}
