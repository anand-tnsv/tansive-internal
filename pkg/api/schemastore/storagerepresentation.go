package schemastore

import (
	"encoding/json"
	"reflect"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type SchemaStorageRepresentation struct {
	Version     string                  `json:"version"`
	Type        types.CatalogObjectType `json:"type"`
	Description string                  `json:"description"`
	Schema      json.RawMessage         `json:"schema"`
	Values      json.RawMessage         `json:"values"`
	Reserved    json.RawMessage         `json:"reserved"`
	Entropy     []byte                  `json:"entropy,omitempty"`
}

// Serialize converts the SchemaStorageRepresentation to a JSON byte array
func (s *SchemaStorageRepresentation) Serialize() ([]byte, apperrors.Error) {
	j, err := json.Marshal(s)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}
	return j, nil
}

func (s *SchemaStorageRepresentation) SetEntropy(entropy []byte) {
	if entropy == nil {
		s.Entropy = nil
		return
	}
	s.Entropy = entropy
}

// GetHash returns the SHA-512 hash of the normalized SchemaStorageRepresentation
func (s *SchemaStorageRepresentation) GetHash() string {
	sz, err := s.Serialize()
	if err != nil {
		return ""
	}
	// Normalize the JSON, so 2 equivalent representations yield the same hash
	nsz, e := NormalizeJSON(sz)
	if e != nil {
		return ""
	}
	hash := HexEncodedSHA512(nsz)
	return hash
}

// Size returns the approximate size of the SchemaStorageRepresentation in bytes
func (s *SchemaStorageRepresentation) Size() int {
	return len(s.Schema) + len(s.Version) + len(s.Type)
}

func (s *SchemaStorageRepresentation) DiffersInSpec(other *SchemaStorageRepresentation) bool {
	if other == nil {
		return true
	}
	// Compare the Schema field only for differences do a byte compare
	res, err := jsonEqual(s.Schema, other.Schema)
	return err != nil || !res
}

func jsonEqual(a, b json.RawMessage) (bool, error) {
	var objA interface{}
	var objB interface{}

	if err := json.Unmarshal([]byte(a), &objA); err != nil {
		return false, err
	}
	if err := json.Unmarshal([]byte(b), &objB); err != nil {
		return false, err
	}

	return reflect.DeepEqual(objA, objB), nil
}
