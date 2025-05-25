package catalogmanager

import (
	"context"
	"net/url"
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

// LoadResourceGroupManagerByPath loads a resource group manager from the database by path.
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

type resourceGroupResource struct {
	req RequestContext
	rgm schemamanager.ResourceGroupManager
}

func (r *resourceGroupResource) Name() string {
	return r.req.ObjectName
}

func (r *resourceGroupResource) Location() string {
	objName := types.ResourceNameFromObjectType(r.req.ObjectType)
	loc := path.Clean("/" + objName + r.rgm.FullyQualifiedName())
	q := url.Values{}
	if namespace := r.rgm.Metadata().Namespace.String(); namespace != "" {
		q.Set("namespace", namespace)
	}
	qStr := q.Encode()
	if qStr != "" {
		loc += "?" + qStr
	}
	return loc
}

func (r *resourceGroupResource) Manager() schemamanager.ResourceGroupManager {
	return r.rgm
}

func (r *resourceGroupResource) Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   r.req.Catalog,
		Variant:   types.NullableStringFrom(r.req.Variant),
		Namespace: types.NullableStringFrom(r.req.Namespace),
	}

	resourceGroup, err := NewResourceGroupManager(ctx, rsrcJson, m)
	if err != nil {
		return "", err
	}
	err = resourceGroup.Save(ctx)
	if err != nil {
		return "", err
	}

	r.req.ObjectName = resourceGroup.Metadata().Name
	r.req.ObjectPath = resourceGroup.Metadata().Path
	r.req.ObjectType = types.CatalogObjectTypeResourceGroup
	r.rgm = resourceGroup

	if r.req.Catalog == "" {
		r.req.Catalog = resourceGroup.Metadata().Catalog
	}
	if r.req.Variant == "" {
		r.req.Variant = resourceGroup.Metadata().Variant.String()
	}
	if r.req.Namespace == "" {
		r.req.Namespace = resourceGroup.Metadata().Namespace.String()
	}

	return r.Location(), nil
}

// Get returns the resource group as JSON
func (r *resourceGroupResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	m := &schemamanager.SchemaMetadata{
		Catalog:   r.req.Catalog,
		Variant:   types.NullableStringFrom(r.req.Variant),
		Namespace: types.NullableStringFrom(r.req.Namespace),
		Path:      r.req.ObjectPath,
		Name:      r.req.ObjectName,
	}
	verr := m.Validate()
	if verr != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(verr.Error())
	}

	rgm, err := LoadResourceGroupManagerByPath(ctx, m)
	if err != nil {
		return nil, err
	}

	return rgm.JSON(ctx)
}

func (r *resourceGroupResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	m := &schemamanager.SchemaMetadata{
		Catalog:   r.req.Catalog,
		Variant:   types.NullableStringFrom(r.req.Variant),
		Path:      r.req.ObjectPath,
		Name:      r.req.ObjectName,
		Namespace: types.NullableStringFrom(r.req.Namespace),
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	// Load the existing object
	existing, err := LoadResourceGroupManagerByPath(ctx, m)
	if err != nil {
		return err
	}
	if existing == nil {
		return ErrObjectNotFound
	}

	rgm, err := NewResourceGroupManager(ctx, rsrcJson, m)
	if err != nil {
		return err
	}
	err = rgm.Save(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (rg *resourceGroupResource) Delete(ctx context.Context) apperrors.Error {
	m := &schemamanager.SchemaMetadata{
		Catalog:   rg.req.Catalog,
		Variant:   types.NullableStringFrom(rg.req.Variant),
		Path:      rg.req.ObjectPath,
		Name:      rg.req.ObjectName,
		Namespace: types.NullableStringFrom(rg.req.Namespace),
	}

	err := DeleteResourceGroup(ctx, m)
	if err != nil {
		pathWithName := path.Clean(m.GetStoragePath(rg.req.ObjectType) + "/" + rg.req.ObjectName)
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("failed to delete object")
		return err
	}
	return nil
}

func (rg *resourceGroupResource) List(ctx context.Context) ([]byte, apperrors.Error) {
	return nil, nil
}

func NewResourceGroupResource(ctx context.Context, req RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if req.Catalog == "" {
		return nil, ErrInvalidCatalog
	}
	if req.Variant == "" {
		return nil, ErrInvalidVariant
	}
	return &resourceGroupResource{
		req: req,
	}, nil
}
