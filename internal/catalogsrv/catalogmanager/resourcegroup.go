package catalogmanager

import (
	"context"
	"encoding/json"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

func NewResourceGroupManager(ctx context.Context, rsrcJson []byte, m *schemamanager.SchemaMetadata) (schemamanager.ResourceGroupManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	// Get the metadata, replace fields in JSON from provided metadata, and set defaults.
	rsrcJson, m, err := canonicalizeMetadata(rsrcJson, types.ResourceGroupKind, m)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}

	var rg ResourceGroup
	if err := json.Unmarshal(rsrcJson, &rg); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return nil, validationerrors.ErrSchemaValidation
	}

	if ves := rg.Validate(); ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	rg.Metadata = *m

	return &resourceGroupManager{resourceGroup: rg}, nil
}

func SaveResourceGroup(ctx context.Context, rgm schemamanager.ResourceGroupManager) apperrors.Error {
	//
	return nil
}
