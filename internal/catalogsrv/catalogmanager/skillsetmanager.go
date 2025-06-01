package catalogmanager

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	json "github.com/json-iterator/go"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/pkg/types"
	"gopkg.in/yaml.v3"
)

// SkillSetKind represents the type of skillset
type SkillSetKind string

const (
	// SkillSetKindResource represents a resource type skillset
	SkillSetKindResource SkillSetKind = "Resource"
)

// Resource represents a single resource in the catalog system.
// It contains metadata, schema, and value information.
type SkillSet struct {
	Version  string              `json:"version" validate:"required,requireVersionV1"`
	Kind     SkillSetKind        `json:"kind" validate:"required,oneof=Resource"`
	Metadata interfaces.Metadata `json:"metadata" validate:"required"`
	Spec     SkillSetSpec        `json:"spec,omitempty"` // we can have empty collections
}

// ResourceSpec defines the specification for a resource, including its schema,
// value, policy, and annotations.
type SkillSetSpec struct {
	Provider  SkillSetProvider   `json:"provider" validate:"required_without=Schema,omitempty,resourceNameValidator"`
	Resources []SkillSetResource `json:"resources" validate:"required,dive"`
	Skills    []Skill            `json:"skills" validate:"required,dive"`
	Uses      Capabilities       `json:"uses" validate:"required"`
}

type SkillSetResource struct {
	Name     string            `json:"name" validate:"required,resourceNameValidator"`
	Provider ResourceProvider  `json:"-" validate:"required_without=Schema,omitempty,resourceNameValidator"`
	Schema   json.RawMessage   `json:"schema" validate:"required_without=Provider,omitempty"`
	Value    types.NullableAny `json:"value" validate:"omitempty"`
}

type SkillSetProvider struct {
	// will contain provider configuration
}

type Skill struct {
	Name            string           `json:"name" validate:"required,resourceNameValidator"`
	Description     string           `json:"description" validate:"required"`
	InputSchema     json.RawMessage  `json:"input_schema" validate:"required"`
	ExportedActions []ExportedAction `json:"exported_actions" validate:"required,dive,resourceNameValidator"`
}

type ResourceReference struct {
	ResourcePath string `json:"resource_path" validate:"required,resourcePathValidator"`
}

type SkillReference struct {
	SkillPath string `json:"skill_path" validate:"required,resourcePathValidator"`
}

type Capabilities struct {
	Resources []ResourceReference `json:"resources" validate:"required,dive"`
	Skills    []SkillReference    `json:"skills" validate:"required,dive"`
}

type ExportedAction string

// Sample yaml:
/*
version: v1
kind: SkillSet
metadata:
  name: example-skillset
  namespace: default
  path: /skillsets/example-skillset
spec:
  provider:
    # Provider configuration would go here
  resources:
    - name: ephemeral-store
      provider:
        # Resource provider configuration
      schema: |
        {
          "type": "object",
          "properties": {
            "key": { "type": "string" },
            "value": { "type": "string" }
          }
        }
	  value:
		key: "default-key"
		value: "default-value"
  skills:
    - name: process-data
      description: "Process and transform data using predefined rules"
      input_schema: |
        {
          "type": "object",
          "properties": {
            "input": { "type": "string" },
            "options": { "type": "object" }
          }
        }
      exported_actions:
        - dataset.transform
        - dataset.validate
        - dataset.export
    - name: analyze-data
      description: "Analyze data and generate insights"
      input_schema: |
        {
          "type": "object",
          "properties": {
            "data": { "type": "array" },
            "analysis_type": { "type": "string" }
          }
        }
      exported_actions:
        - dataset.analyze
        - dataset.report
  uses:
    resources:
      - resource_path: "/resources/database/oracle"
      - resource_path: "/resources/storage/s3"
    skills:
      - skill_path: "/skills/data-processing"
      - skill_path: "/skills/validation"
*/

// NewSkillSet creates a new SkillSet with the given parameters
func NewSkillSet(name, namespace string, resources []SkillSetResource, skills []Skill, capabilities Capabilities) *SkillSet {
	return &SkillSet{
		Version: "v1",
		Kind:    SkillSetKindResource,
		Metadata: interfaces.Metadata{
			Name:      name,
			Namespace: types.NullableStringFrom(namespace),
			Path:      "/skillsets/" + name,
		},
		Spec: SkillSetSpec{
			Resources: resources,
			Skills:    skills,
			Uses:      capabilities,
		},
	}
}

// ToJSON marshals the SkillSet to JSON bytes
func (s *SkillSet) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *SkillSet) ToYAML() ([]byte, error) {
	return yaml.Marshal(s)
}

func (s *SkillSet) FromJSON(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *SkillSet) FromYAML(data []byte) error {
	return yaml.Unmarshal(data, s)
}

func (s *SkillSet) Validate() error {
	validate := validator.New()
	err := validate.Struct(s)
	if err == nil {
		return nil
	}

	// Handle validation errors
	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		var errMsgs []string
		for _, e := range validationErrors {
			field := e.Field()
			tag := e.Tag()
			param := e.Param()
			value := e.Value()

			var msg string
			switch tag {
			case "required":
				msg = fmt.Sprintf("field '%s' is required", field)
			case "oneof":
				msg = fmt.Sprintf("field '%s' must be one of: %s", field, param)
			case "resourceNameValidator":
				msg = fmt.Sprintf("field '%s' must be a valid resource name (allowed characters: [a-z0-9-])", field)
			case "resourcePathValidator":
				msg = fmt.Sprintf("field '%s' must be a valid resource path", field)
			case "requireVersionV1":
				msg = fmt.Sprintf("field '%s' must be 'v1'", field)
			default:
				msg = fmt.Sprintf("field '%s' failed validation: %s", field, tag)
			}

			if value != nil {
				msg += fmt.Sprintf(" (got: %v)", value)
			}
			errMsgs = append(errMsgs, msg)
		}
		return fmt.Errorf("validation failed:\n%s", strings.Join(errMsgs, "\n"))
	}

	return fmt.Errorf("validation error: %w", err)
}
