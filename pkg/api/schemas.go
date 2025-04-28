package api

import (
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/pkg/types"
)

// TODO: Several of the types are defined in internal packages. Need to move them to pkg/api or pkg/types
type CatalogSchema struct {
	Version  string          `json:"version"`
	Kind     string          `json:"kind"`
	Metadata CatalogMetadata `json:"metadata"`
}

type CatalogMetadata struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type VariantSchema struct {
	Version  string          `json:"version"`
	Kind     string          `json:"kind"`
	Metadata VariantMetadata `json:"metadata"`
}

type VariantMetadata struct {
	Name        string `json:"name"`
	Catalog     string `json:"catalog"`
	Description string `json:"description"`
}

type NamespaceSchema struct {
	Version  string            `json:"version"`
	Kind     string            `json:"kind"`
	Metadata NamespaceMetadata `json:"metadata"`
}

type NamespaceMetadata struct {
	Catalog     string `json:"catalog"`
	Variant     string `json:"variant"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type WorkspaceSchema struct {
	Version  string            `json:"version"`
	Kind     string            `json:"kind"`
	Metadata WorkspaceMetadata `json:"metadata"`
}

type WorkspaceMetadata struct {
	Catalog     string `json:"catalog"`
	Variant     string `json:"variant"`
	BaseVersion int    `json:"-"`
	Description string `json:"description"`
	Label       string `json:"label"`
}

type SchemaMetadata struct {
	Name        string `json:"name"`
	Path        string `json:"path,omitempty"`
	Description string `json:"description"`
}

type CollectionSchema struct {
	Version string                    `json:"version"`
	Spec    CollectionSchemaSpec      `json:"spec,omitempty"` // we can have empty collections
	Values  schemamanager.ParamValues `json:"-"`
}

type CollectionSchemaSpec struct {
	Parameters map[string]Parameter `json:"parameters,omitempty"`
}

type Parameter struct {
	Schema      string                    `json:"schema"`
	DataType    string                    `json:"dataType"`
	Default     types.NullableAny         `json:"default"`
	Annotations schemamanager.Annotations `json:"annotations"`
}

type AttributeSchema struct {
	Version  string              `json:"version"`
	Kind     string              `json:"kind"`
	Metadata SchemaMetadata      `json:"metadata"`
	Spec     AttributeSchemaSpec `json:"spec,omitempty"`
}

type AttributeSchemaSpec struct {
	DataType   string            `json:"dataType"`
	Validation json.RawMessage   `json:"validation"`
	Default    types.NullableAny `json:"default"`
}

type Collection struct {
	Version    string                    `json:"version"`
	Kind       string                    `json:"kind"`
	Metadata   SchemaMetadata            `json:"metadata"`
	Spec       CollectionSpec            `json:"spec"`
	Values     schemamanager.ParamValues `json:"-"`
	SchemaPath string                    `json:"-"`
}

type CollectionSpec struct {
	Schema string                       `json:"schema"`
	Values map[string]types.NullableAny `json:"values"`
}

type SetAttributeReq struct {
	Value types.NullableAny `json:"value"`
}
type SetCollectionValuesReq struct {
	Values map[string]types.NullableAny `json:"values"`
}

type GetAttributeRsp map[string]schemamanager.ParamValue
type GetAllAttributesRsp struct {
	Values map[string]schemamanager.ParamValue `json:"values"`
}
