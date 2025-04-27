package schemamanager

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/api/schemastore"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ParamDataType struct {
	Type    string `json:"type"`
	Version string `json:"version"`
}

func (dt ParamDataType) Equals(other ParamDataType) bool {
	return dt.Type == other.Type && dt.Version == other.Version
}

type ParamValue struct {
	Value       types.NullableAny `json:"value"`
	DataType    ParamDataType     `json:"data_type"`
	Annotations Annotations       `json:"annotations"`
}

func (pv ParamValue) ToJson() ([]byte, error) {
	if pv.Value.IsNil() {
		return json.Marshal(nil)
	}
	return json.Marshal(pv)
}

func (pv ParamValue) Equals(other ParamValue) bool {
	if !pv.Value.Equals(other.Value) {
		return false
	}

	if !pv.DataType.Equals(other.DataType) {
		return false
	}

	if len(pv.Annotations) != len(other.Annotations) {
		return false
	}

	for k, v := range pv.Annotations {
		if ov, ok := other.Annotations[k]; !ok || ov != v {
			return false
		}
	}

	return true
}

type ParamValues map[string]ParamValue
type Annotations map[string]any

type ParameterSchemaManager interface {
	DataType() ParamDataType
	Default() any
	ValidateValue(types.NullableAny) apperrors.Error
	ValidateDependencies(ctx context.Context, loaders SchemaLoaders, collectionRefs SchemaReferences) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
