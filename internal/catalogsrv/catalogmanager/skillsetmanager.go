package catalogmanager

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"

	"encoding/json"

	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/objectstore"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

type DependencyKind string

const (
	KindSkill    DependencyKind = "Skill"
	KindResource DependencyKind = "Resource"
)

// SkillSet represents a single skillset in the catalog system.
// It contains metadata, schema, and value information.
type SkillSet struct {
	Version  string              `json:"version" validate:"required,requireVersionV1"` //API Version
	Kind     string              `json:"kind" validate:"required,oneof=SkillSet"`
	Metadata interfaces.Metadata `json:"metadata" validate:"required"`
	Spec     SkillSetSpec        `json:"spec,omitempty"`
}

// SkillSetSpec defines the specification for a skillset, including its schema,
// value, policy, and annotations.
type SkillSetSpec struct {
	Version      string            `json:"version" validate:"required"`
	Runner       SkillSetRunner    `json:"runner" validate:"required,omitempty"`
	Context      []SkillSetContext `json:"context" validate:"omitempty,dive"`
	Skills       []Skill           `json:"skills" validate:"required,dive"`
	Dependencies []Dependency      `json:"dependencies,omitempty" validate:"omitempty,dive"`
	Annotations  map[string]string `json:"annotations,omitempty" validate:"omitempty"`
}

type SkillSetContext struct {
	Name     string            `json:"name" validate:"required,resourceNameValidator"`
	Provider ResourceProvider  `json:"provider,omitempty" validate:"required_without=Schema,omitempty,resourceNameValidator"`
	Schema   json.RawMessage   `json:"schema" validate:"required_without=Provider,omitempty,jsonSchemaValidator"`
	Value    types.NullableAny `json:"value" validate:"omitempty"`
}

type SkillSetRunner struct {
	ID     catcommon.RunnerID `json:"id" validate:"required"`
	Config map[string]any     `json:"config" validate:"required"`
}

type Skill struct {
	Name            string            `json:"name" validate:"required,skillNameValidator"`
	Description     string            `json:"description" validate:"required"`
	InputSchema     json.RawMessage   `json:"inputSchema" validate:"required,jsonSchemaValidator"`
	OutputSchema    json.RawMessage   `json:"outputSchema" validate:"required,jsonSchemaValidator"`
	ExportedActions []policy.Action   `json:"exportedActions" validate:"required,dive"`
	Annotations     map[string]string `json:"annotations" validate:"omitempty"`
}

type Dependency struct {
	Path    string          `json:"path" validate:"required,resourcePathValidator"`
	Kind    DependencyKind  `json:"kind" validate:"required,oneof=SkillSet Resource"`
	Alias   string          `json:"alias" validate:"required,resourceNameValidator"`
	Actions []policy.Action `json:"actions" validate:"required,dive"`
}

// SkillMetadata represents the metadata extracted from skills and dependencies
type SkillMetadata struct {
	Skills       []SkillSummary `json:"skills"`
	Dependencies []Dependency   `json:"dependencies"`
}

type SkillSummary struct {
	Name            string          `json:"name"`
	ExportedActions []policy.Action `json:"exportedActions"`
}

func (m *SkillMetadata) GetSkill(name string) (SkillSummary, bool) {
	for _, skill := range m.Skills {
		if skill.Name == name {
			return skill, true
		}
	}
	return SkillSummary{}, false
}

// skillSetManager implements the SkillSetManager interface for managing a single skillset.
type skillSetManager struct {
	skillSet SkillSet
}

// Metadata returns the skillset's metadata.
func (sm *skillSetManager) Metadata() interfaces.Metadata {
	return sm.skillSet.Metadata
}

// FullyQualifiedName returns the fully qualified name of the skillset.
func (sm *skillSetManager) FullyQualifiedName() string {
	m := sm.skillSet.Metadata
	return path.Clean(m.Path + "/" + m.Name)
}

// StorageRepresentation returns the object storage representation of the skillset.
func (sm *skillSetManager) StorageRepresentation() *objectstore.ObjectStorageRepresentation {
	s := objectstore.ObjectStorageRepresentation{
		Version: sm.skillSet.Version,
		Type:    catcommon.CatalogObjectTypeSkillset,
	}
	s.Spec, _ = json.Marshal(sm.skillSet.Spec)
	s.Description = sm.skillSet.Metadata.Description
	s.Entropy = sm.skillSet.Metadata.GetEntropyBytes(catcommon.CatalogObjectTypeSkillset)
	return &s
}

// GetStoragePath returns the storage path for the skillset.
func (sm *skillSetManager) GetStoragePath() string {
	m := sm.Metadata()
	return getSkillSetStoragePath(&m)
}

func (sm *skillSetManager) GetResourcePath() string {
	return "/skillsets/" + sm.Metadata().GetFullyQualifiedName()
}

// getSkillSetStoragePath constructs the storage path for a skillset based on its metadata.
func getSkillSetStoragePath(m *interfaces.Metadata) string {
	t := catcommon.CatalogObjectTypeSkillset
	ssPath := m.GetStoragePath(t)
	pathWithName := path.Clean(ssPath + "/" + m.Name)
	return pathWithName
}

// GetSkillMetadata constructs metadata from skills and dependencies
func (sm *skillSetManager) GetSkillMetadata() (SkillMetadata, apperrors.Error) {
	metadata := SkillMetadata{
		Skills:       make([]SkillSummary, 0, len(sm.skillSet.Spec.Skills)),
		Dependencies: sm.skillSet.Spec.Dependencies,
	}

	// Extract skill names and exported actions
	for _, skill := range sm.skillSet.Spec.Skills {
		metadata.Skills = append(metadata.Skills, SkillSummary{
			Name:            skill.Name,
			ExportedActions: skill.ExportedActions,
		})
	}

	return metadata, nil
}

// Save saves the skillset to the database.
// It handles the creation or update of both the skillset and its associated catalog object.
func (sm *skillSetManager) Save(ctx context.Context) apperrors.Error {
	if sm == nil {
		return ErrEmptySchema
	}

	t := catcommon.CatalogObjectTypeSkillset

	m := sm.Metadata()
	s := sm.StorageRepresentation()
	storagePath := sm.GetStoragePath()

	data, err := s.Serialize()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to serialize skillset")
		return err
	}
	newHash := s.GetHash()

	// Get skill metadata
	skillMetadata, err := sm.GetSkillMetadata()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to generate skill metadata")
		return ErrUnableToLoadObject.Msg("failed to generate skill metadata")
	}

	// Store this object and update the reference
	obj := models.CatalogObject{
		Type:    t,
		Hash:    newHash,
		Data:    data,
		Version: sm.skillSet.Version,
	}

	// Get the directory ID for the skillset
	catalogID := catcommon.GetCatalogID(ctx)
	if catalogID == uuid.Nil {
		var err apperrors.Error
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, m.Catalog)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("catalog", m.Catalog).Msg("Failed to get catalog ID by name")
			return err
		}
	}

	variant, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, m.Variant.String())
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("catalogID", catalogID.String()).Str("name", m.Name).Msg("Failed to get variant")
		return err
	}

	skillMetadataJSON, goerr := json.Marshal(skillMetadata)
	if goerr != nil {
		log.Ctx(ctx).Error().Err(goerr).Msg("Failed to marshal skill metadata")
		return ErrUnableToLoadObject.Msg("failed to marshal skill metadata")
	}

	// Create the skillset model
	ss := &models.SkillSet{
		Path:      storagePath,
		Hash:      newHash,
		VariantID: variant.VariantID,
		Metadata:  skillMetadataJSON,
	}

	// Store the object
	err = db.DB(ctx).UpsertSkillSetObject(ctx, ss, &obj, variant.SkillsetDirectoryID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("path", storagePath).Msg("Failed to store object")
		return err
	}

	return nil
}

// DeleteSkillSet deletes a skillset from the database.
func DeleteSkillSet(ctx context.Context, m *interfaces.Metadata) apperrors.Error {
	if m == nil {
		return ErrInvalidObject.Msg("unable to infer object metadata")
	}

	// Get the directory ID for the skillset
	catalogID := catcommon.GetCatalogID(ctx)
	if catalogID == uuid.Nil {
		var err apperrors.Error
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, m.Catalog)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("catalog", m.Catalog).Msg("Failed to get catalog ID by name")
			return err
		}
	}

	variant, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, m.Variant.String())
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("catalogID", catalogID.String()).Str("name", m.Name).Msg("Failed to get variant")
		return err
	}

	pathWithName := path.Clean(m.GetStoragePath(catcommon.CatalogObjectTypeSkillset) + "/" + m.Name)

	// Delete the skillset
	hash, err := db.DB(ctx).DeleteSkillSet(ctx, pathWithName, variant.SkillsetDirectoryID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrObjectNotFound
		}
		log.Ctx(ctx).Error().Err(err).Str("path", pathWithName).Msg("Failed to delete object")
		return err
	}

	if hash != "" {
		err = db.DB(ctx).DeleteCatalogObject(ctx, catcommon.CatalogObjectTypeSkillset, hash)
		if !errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Error().Err(err).Str("hash", string(hash)).Msg("failed to delete object from database")
		}
	} else {
		log.Ctx(ctx).Warn().Str("path", pathWithName).Msg("skillset not found")
		return ErrObjectNotFound
	}

	return nil
}

// JSON returns the JSON representation of the skillset.
func (sm *skillSetManager) JSON(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(sm.skillSet)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to marshal skillset")
		return nil, ErrUnableToLoadObject
	}
	return j, nil
}

// SpecJSON returns the JSON representation of the skillset's spec.
func (sm *skillSetManager) SpecJSON(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(sm.skillSet.Spec)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("Failed to marshal skillset spec")
		return nil, ErrInvalidSkillSetDefinition
	}
	return j, nil
}

// Validate performs validation on the skillset, including:
// - Kind validation
// - Schema validation
// - Value validation against the schema
func (s *SkillSet) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	if s.Kind != catcommon.SkillSetKind {
		validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind("kind"))
	}

	err := schemavalidator.V().Struct(s)
	if err == nil {
		// Validate each skill's input and output schemas
		for _, skill := range s.Spec.Skills {
			// Validate input schema
			if len(skill.InputSchema) > 0 {
				_, err := compileSchema(string(skill.InputSchema))
				if err != nil {
					validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(fmt.Sprintf("skill %s input schema: %v", skill.Name, err)))
				}
			}

			// Validate output schema
			if len(skill.OutputSchema) > 0 {
				_, err := compileSchema(string(skill.OutputSchema))
				if err != nil {
					validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(fmt.Sprintf("skill %s output schema: %v", skill.Name, err)))
				}
			}
		}

		// Validate each context's schema
		for _, ctx := range s.Spec.Context {
			if len(ctx.Schema) > 0 {
				_, err := compileSchema(string(ctx.Schema))
				if err != nil {
					validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(fmt.Sprintf("context %s schema: %v", ctx.Name, err)))
				}
			}
		}
		return validationErrors
	}

	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(validationErrors, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(s).Elem()
	typeOfCS := value.Type()

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute(e.StructNamespace()))
		case "oneof":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidFieldSchema(jsonFieldName, e.Value().(string)))
		case "resourceNameValidator":
			val, _ := e.Value().(string)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "jsonSchemaValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidFieldSchema(jsonFieldName))
		default:
			val := e.Value()
			param := e.Param()
			s := fmt.Sprintf("%v: %v", param, val)
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(s))
		}
	}
	return validationErrors
}
