package datatyperegistry

import (
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type Loader func([]byte) (schemamanager.DataType, apperrors.Error)

var registry = make(map[schemamanager.ParamDataType]Loader)

func RegisterDataType(k schemamanager.ParamDataType, ld Loader) {
	registry[k] = ld
}

func GetLoader(k schemamanager.ParamDataType) Loader {
	return registry[k]
}

func DataTypeExists(k schemamanager.ParamDataType) bool {
	_, exists := registry[k]
	return exists
}
