package catalogmanager

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var _ = canonicalizeMetadata
var _ = getMetadata

func getMetadata(ctx context.Context, resourceJSON []byte) (*schemamanager.SchemaMetadata, apperrors.Error) {
	if len(resourceJSON) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	if !gjson.ValidBytes(resourceJSON) {
		return nil, validationerrors.ErrSchemaValidation
	}

	metadata := gjson.GetBytes(resourceJSON, "metadata")
	if !metadata.Exists() {
		return nil, validationerrors.ErrSchemaValidation
	}

	var schemaMetadata schemamanager.SchemaMetadata
	if err := json.Unmarshal([]byte(metadata.Raw), &schemaMetadata); err != nil {
		return nil, validationerrors.ErrSchemaValidation
	}

	return &schemaMetadata, nil
}

func canonicalizeMetadata(resourceJSON []byte, kind string, metadata *schemamanager.SchemaMetadata) ([]byte, *schemamanager.SchemaMetadata, apperrors.Error) {
	if len(resourceJSON) == 0 {
		return nil, nil, validationerrors.ErrEmptySchema
	}

	if !gjson.ValidBytes(resourceJSON) {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("invalid JSON")
	}

	metadataResult := gjson.GetBytes(resourceJSON, "metadata")
	if !metadataResult.Exists() {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("missing metadata in resource schema")
	}

	var resourceMetadata schemamanager.SchemaMetadata
	if err := json.Unmarshal([]byte(metadataResult.Raw), &resourceMetadata); err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal metadata")
	}

	if metadata != nil {
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

	updatedJSON, err := sjson.SetBytes(resourceJSON, "metadata", resourceMetadata)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to update metadata")
	}

	return updatedJSON, &resourceMetadata, nil
}
