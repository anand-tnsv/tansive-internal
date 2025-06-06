package session

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"io"
	"path"
	"reflect"
	"time"

	"github.com/go-playground/validator/v10"
	json "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tidwall/gjson"
)

// SessionSpec defines the structure for session creation requests
type SessionSpec struct {
	SkillPath string          `json:"skillPath" validate:"required,resourcePathValidator"`
	ViewName  string          `json:"viewName" validate:"required,resourceNameValidator"`
	Variables json.RawMessage `json:"variables" validate:"omitempty"`
}

// variableSchema defines the JSON schema for session variables
const variableSchema = `
{
  "type": "object",
  "maxProperties": %d,
  "propertyNames": {
    "pattern": "^[a-zA-Z0-9.-]+$"
  },
  "additionalProperties": {
    "type": ["string", "number", "boolean", "object", "array", "null"]
  }
}`

var variableSchemaCompiled *jsonschema.Schema

// sessionManager implements the SessionManager interface
type sessionManager struct {
	session         *models.Session
	skillSetManager catalogmanager.SkillSetManager
	viewManager     policy.ViewManager
}

func init() {
	schema := fmt.Sprintf(variableSchema, config.Config().Session.MaxVariables)
	compiledSchema, err := compileSchema(schema)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to compile session variables schema")
	}
	variableSchemaCompiled = compiledSchema
}

// NewSession creates a new session with the given specification. It validates the session spec,
// checks permissions, and initializes the session with the provided configuration.
// The function requires valid catalog ID, variant ID, and user ID in the context.
// Returns a SessionManager interface and any error that occurred during creation.
func NewSession(ctx context.Context, rsrcSpec []byte) (SessionManager, apperrors.Error) {
	// Validate required IDs first
	catalogID := catcommon.GetCatalogID(ctx)
	variantID := catcommon.GetVariantID(ctx)
	if catalogID == uuid.Nil || variantID == uuid.Nil {
		return nil, ErrInvalidObject.Msg("catalog and variant IDs are required")
	}

	userID := catcommon.GetUserID(ctx)
	if userID == "" {
		return nil, ErrInvalidObject.Msg("user ID is required")
	}

	// Parse and validate session spec
	sessionSpec, err := resolveSessionSpec(rsrcSpec)
	if err != nil {
		return nil, err
	}

	// Get skill and skill set path from session spec
	skill := path.Base(sessionSpec.SkillPath)
	skillSetPath := path.Dir(sessionSpec.SkillPath)
	if skill == "" || skillSetPath == "" {
		return nil, ErrInvalidObject.Msg("invalid skill path")
	}

	// Validate view policy
	if err := validateViewPolicy(ctx, sessionSpec.ViewName); err != nil {
		return nil, err
	}

	// Initialize view manager
	viewManager, err := resolveViewByLabel(ctx, sessionSpec.ViewName)
	if err != nil {
		return nil, err
	}

	// Get and validate view definition
	viewDef, err := viewManager.GetViewDefinition(ctx)
	if err != nil {
		return nil, err
	}
	viewDefJSON, err := viewManager.GetViewDefinitionJSON(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize skill set manager
	skillSetManager, err := resolveSkillSet(ctx, skillSetPath)
	if err != nil {
		return nil, err
	}

	// Get and validate skill metadata
	skillSetMetadata, err := skillSetManager.GetSkillMetadata()
	if err != nil {
		return nil, err
	}

	skillSummary, ok := skillSetMetadata.GetSkill(skill)
	if !ok {
		return nil, ErrInvalidObject.Msg("skill not found in skillset")
	}

	// Validate action permissions
	exportedActions := skillSummary.ExportedActions
	allowed, err := policy.AreActionsAllowedOnResource(ctx, viewDef, skillSetManager.GetResourcePath(), exportedActions)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrDisallowedByPolicy.Msg("use of skill is blocked by policy")
	}

	// Create session
	sessionID := uuid.New()
	session := &models.Session{
		SessionID:      sessionID,
		SkillSet:       skillSetPath,
		Skill:          skill,
		ViewID:         viewManager.ID(),
		ViewDefinition: viewDefJSON,
		Variables:      gojson.RawMessage(sessionSpec.Variables),
		StatusSummary:  SessionStatusCreated,
		Status:         nil,
		Info:           nil,
		UserID:         userID,
		CatalogID:      catalogID,
		VariantID:      variantID,
		StartedAt:      time.Now(),
		EndedAt:        time.Time{},
		ExpiresAt:      time.Now().Add(config.Config().Session.ExpirationTime),
	}

	return &sessionManager{
		session:         session,
		skillSetManager: skillSetManager,
		viewManager:     viewManager,
	}, nil
}

// Save persists the session to the database.
// Returns an error if the save operation fails.
func (s *sessionManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateSession(ctx, s.session)
	if err != nil {
		return ErrInvalidObject.Msg("failed to save session: " + err.Error())
	}
	return nil
}

// GetSession retrieves an existing session by its ID.
// Returns a SessionManager interface and any error that occurred during retrieval.
func GetSession(ctx context.Context, sessionID uuid.UUID) (SessionManager, apperrors.Error) {
	session, err := db.DB(ctx).GetSession(ctx, sessionID)
	if err != nil {
		return nil, ErrInvalidObject.Msg("failed to get session: " + err.Error())
	}
	viewManager, err := resolveViewByID(ctx, session.ViewID)
	if err != nil {
		return nil, err
	}
	skillSetManager, err := resolveSkillSet(ctx, session.SkillSet)
	if err != nil {
		return nil, err
	}
	sm := &sessionManager{
		session:         session,
		skillSetManager: skillSetManager,
		viewManager:     viewManager,
	}
	return sm, nil
}

// resolveSessionSpec parses and validates the session specification
func resolveSessionSpec(rsrcSpec []byte) (SessionSpec, apperrors.Error) {
	sessionSpec := SessionSpec{}
	if err := json.Unmarshal(rsrcSpec, &sessionSpec); err != nil {
		return sessionSpec, ErrInvalidSession.Msg("invalid session spec: " + err.Error())
	}

	// Validate the session spec
	validationErrors := sessionSpec.Validate()
	if len(validationErrors) > 0 {
		return sessionSpec, ErrInvalidSession.Msg(validationErrors.Error())
	}

	return sessionSpec, nil
}

// validateSessionVariables validates the session variables against the schema
func validateSessionVariables(variables json.RawMessage) schemaerr.ValidationErrors {
	if len(variables) == 0 {
		return nil
	}

	var parsed any
	if err := json.Unmarshal(variables, &parsed); err != nil {
		return schemaerr.ValidationErrors{schemaerr.ErrValidationFailed("invalid session variables: " + err.Error())}
	}

	if err := variableSchemaCompiled.Validate(parsed); err != nil {
		msg := fmt.Sprintf("session variables must be key-value json objects with max %d properties: %v", config.Config().Session.MaxVariables, err)
		return schemaerr.ValidationErrors{schemaerr.ErrValidationFailed(msg)}
	}

	return nil
}

// validateSessionSpecFields validates the session spec fields using the validator
func validateSessionSpecFields(s *SessionSpec) schemaerr.ValidationErrors {
	err := schemavalidator.V().Struct(s)
	if err == nil {
		return nil
	}

	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return schemaerr.ValidationErrors{schemaerr.ErrInvalidSchema}
	}

	value := reflect.ValueOf(s).Elem()
	typeOfCS := value.Type()
	var validationErrors schemaerr.ValidationErrors

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "resourceNameValidator":
			val, _ := e.Value().(string)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		default:
			val := e.Value()
			param := e.Param()
			s := fmt.Sprintf("%v: %v", param, val)
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(s))
		}
	}
	return validationErrors
}

// Validate validates the session specification against required rules and constraints.
// Checks both the struct fields and session variables for validity.
// Returns a collection of validation errors if any are found.
func (s *SessionSpec) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors

	// Validate struct fields
	if errs := validateSessionSpecFields(s); len(errs) > 0 {
		validationErrors = append(validationErrors, errs...)
	}

	// Validate variables
	if errs := validateSessionVariables(s.Variables); len(errs) > 0 {
		validationErrors = append(validationErrors, errs...)
	}

	return validationErrors
}

// resolveViewByLabel creates a new view manager for the given view name
func resolveViewByLabel(ctx context.Context, viewName string) (policy.ViewManager, apperrors.Error) {
	viewManager, err := policy.NewViewManagerByViewLabel(ctx, viewName)
	if err != nil {
		return nil, err
	}
	return viewManager, nil
}

// resolveViewByID creates a new view manager for the given view ID
func resolveViewByID(ctx context.Context, viewID uuid.UUID) (policy.ViewManager, apperrors.Error) {
	viewManager, err := policy.NewViewManagerByViewID(ctx, viewID)
	if err != nil {
		return nil, err
	}
	return viewManager, nil
}

// resolveSkillSet creates a new skill set manager for the given path
func resolveSkillSet(ctx context.Context, skillSetPath string) (catalogmanager.SkillSetManager, apperrors.Error) {
	if skillSetPath == "" {
		return nil, ErrInvalidObject.Msg("skillset path is required")
	}

	skillSetManager, err := catalogmanager.GetSkillSetManager(ctx, skillSetPath)
	if err != nil {
		return nil, err
	}
	return skillSetManager, nil
}

// compileSchema compiles a JSON schema string into a jsonschema.Schema
func compileSchema(schema string) (*jsonschema.Schema, error) {
	if !gjson.Valid(schema) {
		return nil, fmt.Errorf("invalid JSON schema")
	}

	compiler := jsonschema.NewCompiler()
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		if url == "inline://schema" {
			return io.NopCloser(bytes.NewReader([]byte(schema))), nil
		}
		return nil, fmt.Errorf("unsupported schema ref: %s", url)
	}
	err := compiler.AddResource("inline://schema", bytes.NewReader([]byte(schema)))
	if err != nil {
		return nil, fmt.Errorf("failed to add schema resource: %w", err)
	}
	compiledSchema, err := compiler.Compile("inline://schema")
	if err != nil {
		return nil, fmt.Errorf("failed to compile schema: %w", err)
	}

	return compiledSchema, nil
}

func validateViewPolicy(ctx context.Context, view string) apperrors.Error {
	if view == "" {
		return ErrInvalidObject.Msg("view is required")
	}

	allowed, err := policy.CanAdoptView(ctx, view)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrDisallowedByPolicy.Msg("view is not allowed to be adopted")
	}

	return nil
}
