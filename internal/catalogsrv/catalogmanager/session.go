package catalogmanager

import (
	json "github.com/json-iterator/go"
)

type SessionSpec struct {
	SkillPath string          `json:"skillPath" validate:"required,resourcePathValidator"`
	ViewName  string          `json:"viewName" validate:"required,resourceNameValidator"`
	Variables json.RawMessage `json:"variables" validate:"omitempty"`
}

const variableSchema = `
{
  "type": "object",
  "maxProperties": 20,
  "propertyNames": {
    "pattern": "^[a-zA-Z0-9.-]+$"
  },
  "additionalProperties": {
    "type": ["string", "number", "boolean", "object", "array", "null"]
  }
}`
