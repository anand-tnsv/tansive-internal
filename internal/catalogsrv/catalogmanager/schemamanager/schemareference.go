package schemamanager

import (
	"encoding/json"
	"path"
)

// We'll keep this a struct, so this is extensible in the future
type SchemaReference struct {
	Name string `json:"name"`
}

func (pr SchemaReference) String() string {
	return pr.Name
}

func (pr SchemaReference) SchemaName() string {
	return path.Base(pr.Name)
}

func (pr SchemaReference) Path() string {
	return path.Dir(pr.Name)
}

type SchemaReferences []SchemaReference

func (prs SchemaReferences) Serialize() ([]byte, error) {
	s, err := json.Marshal(prs)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func DeserializeSchemaReferences(b []byte) (SchemaReferences, error) {
	prs := SchemaReferences{}
	err := json.Unmarshal(b, &prs)
	return prs, err
}
