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

// preventing undefined use warnings
var _ = canonicalizeMetadata
var _ = getMetadata

func getMetadata(ctx context.Context, resourceJSON []byte) (*schemamanager.SchemaMetadata, apperrors.Error) {
	if len(resourceJSON) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	var resource struct {
		VersionHeader
		Metadata schemamanager.SchemaMetadata `json:"metadata"`
	}
	if err := json.Unmarshal(resourceJSON, &resource); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return nil, validationerrors.ErrSchemaValidation
	}

	return &resource.Metadata, nil
}

func canonicalizeMetadata(resourceJSON []byte, kind string, metadata *schemamanager.SchemaMetadata) ([]byte, *schemamanager.SchemaMetadata, apperrors.Error) {
	if len(resourceJSON) == 0 {
		return nil, nil, validationerrors.ErrEmptySchema
	}

	var resourceMap map[string]json.RawMessage // parse only the first level elements
	if err := json.Unmarshal(resourceJSON, &resourceMap); err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal resource schema")
	}

	rawMetadata, ok := resourceMap["metadata"]
	if !ok {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("missing metadata in resource schema")
	}

	// get metadata in resource json
	var resourceMetadata schemamanager.SchemaMetadata
	if err := json.Unmarshal(rawMetadata, &resourceMetadata); err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal metadata")
	}

	if metadata != nil {
		// update metadata fields with new values
		if metadata.Name != "" {
			resourceMetadata.Name = metadata.Name
		}
		if metadata.Catalog != "" {
			resourceMetadata.Catalog = metadata.Catalog
		}
		if !metadata.Variant.IsNil() {
			resourceMetadata.Variant = metadata.Variant
		}
		if metadata.Path != "" {
			resourceMetadata.Path = metadata.Path
		}
		if metadata.Description != "" {
			resourceMetadata.Description = metadata.Description
		}
		if !metadata.Namespace.IsNil() {
			resourceMetadata.Namespace = metadata.Namespace
		}
	}

	if resourceMetadata.Variant.IsNil() {
		resourceMetadata.Variant = types.NullableStringFrom(types.DefaultVariant) // set default variant if nil
	}

	// marshal updated metadata back to json
	metadataJSON, err := json.Marshal(resourceMetadata)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal metadata")
	}
	resourceMap["metadata"] = metadataJSON

	updatedResourceJSON, err := json.Marshal(resourceMap)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal resource schema")
	}

	return updatedResourceJSON, &resourceMetadata, nil
}
