package catalogmanager

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/rs/zerolog/log"
)

// preventing undefined use warnings
var _ = canonicalizeMetadata
var _ = getMetadata

func getMetadata(ctx context.Context, rsrcJson []byte) (*schemamanager.SchemaMetadata, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	var rs struct {
		VersionHeader
		Metadata schemamanager.SchemaMetadata `json:"metadata"`
	}
	err := json.Unmarshal(rsrcJson, &rs)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return nil, validationerrors.ErrSchemaValidation
	}

	return &rs.Metadata, nil
}

func canonicalizeMetadata(rsrcJson []byte, kind string, metadata *schemamanager.SchemaMetadata) ([]byte, *schemamanager.SchemaMetadata, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, nil, validationerrors.ErrEmptySchema
	}

	var fullMap map[string]json.RawMessage // parse only the first level elements
	if err := json.Unmarshal(rsrcJson, &fullMap); err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal resource schema")
	}
	var (
		rawMetadata json.RawMessage
		ok          bool
	)
	if rawMetadata, ok = fullMap["metadata"]; !ok {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("missing metadata in resource schema")
	}
	// get metadata in resource json
	var m schemamanager.SchemaMetadata
	err := json.Unmarshal(rawMetadata, &m)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal metadata")
	}
	if metadata != nil {
		// update metadata fields with new values
		if metadata.Name != "" {
			m.Name = metadata.Name
		}
		if metadata.Catalog != "" {
			m.Catalog = metadata.Catalog
		}
		if !metadata.Variant.IsNil() {
			m.Variant = metadata.Variant
		}
		if metadata.Path != "" {
			m.Path = metadata.Path
		}
		if metadata.Description != "" {
			m.Description = metadata.Description
		}
		if !metadata.Namespace.IsNil() {
			m.Namespace = metadata.Namespace
		}
	}

	if m.Variant.IsNil() {
		m.Variant = types.NullableStringFrom(types.DefaultVariant) // set default variant if nil
	}

	// marshal updated metadata back to json
	j, err := json.Marshal(m)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal metadata")
	}
	fullMap["metadata"] = j

	rs, err := json.Marshal(fullMap)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal resource schema")
	}

	return rs, &m, nil
}
