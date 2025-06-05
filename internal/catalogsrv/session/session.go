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
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
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
  "maxProperties": 20,
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
	skillSetManager interfaces.SkillSetManager
	viewManager     policy.ViewManager
}

func init() {
	compiledSchema, err := compileSchema(variableSchema)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to compile session variables schema")
	}
	variableSchemaCompiled = compiledSchema
}

// NewSession creates a new session with the given specification
func NewSession(ctx context.Context, rsrcSpec []byte) (SessionManager, apperrors.Error) {
	sessionSpec, err := resolveSessionSpec(rsrcSpec)
	if err != nil {
		return nil, err
	}

	catalogID := catcommon.GetCatalogID(ctx)
	variantID := catcommon.GetVariantID(ctx)
	if catalogID == uuid.Nil || variantID == uuid.Nil {
		return nil, ErrInvalidObject.Msg("catalog and variant IDs are required")
	}

	// Get skill and skill set path from session spec
	skill := path.Base(sessionSpec.SkillPath)
	skillSetPath := path.Dir(sessionSpec.SkillPath)
	if skill == "" || skillSetPath == "" {
		return nil, ErrInvalidObject.Msg("invalid skill path")
	}

	viewManager, err := resolveViewByLabel(ctx, sessionSpec.ViewName)
	if err != nil {
		return nil, err
	}

	viewDef, err := viewManager.GetViewDefinition(ctx)
	if err != nil {
		return nil, err
	}

	// Validate view policy - session can only be created for views that are
	// a subset of the current view
	err = validateViewPolicy(ctx, viewDef)
	if err != nil {
		return nil, err
	}

	viewDefJSON, goerr := viewDef.ToJSON()
	if goerr != nil {
		return nil, ErrInvalidObject.Msg("invalid view definition: " + goerr.Error())
	}

	skillSetManager, err := resolveSkillSet(ctx, skillSetPath)
	if err != nil {
		return nil, err
	}
	//skillSetMetadata, err := skillSetManager.GetSkillMetadata(ctx)

	userID := catcommon.GetUserID(ctx)
	if userID == "" {
		return nil, ErrInvalidObject.Msg("user ID is required")
	}

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
		ExpiresAt:      time.Now().Add(time.Hour * 24),
	}
	sm := &sessionManager{
		session:         session,
		skillSetManager: skillSetManager,
		viewManager:     viewManager,
	}
	return sm, nil
}

func (s *sessionManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateSession(ctx, s.session)
	if err != nil {
		return ErrInvalidObject.Msg("failed to save session: " + err.Error())
	}
	return nil
}

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

// resolveViewByLabel creates a new view manager for the given view name
func resolveViewByLabel(ctx context.Context, viewName string) (policy.ViewManager, apperrors.Error) {
	viewManager, err := policy.NewViewManagerByViewLabel(ctx, viewName)
	if err != nil {
		return nil, err
	}
	return viewManager, nil
}

func resolveViewByID(ctx context.Context, viewID uuid.UUID) (policy.ViewManager, apperrors.Error) {
	viewManager, err := policy.NewViewManagerByViewID(ctx, viewID)
	if err != nil {
		return nil, err
	}
	return viewManager, nil
}

// resolveSkillSet creates a new skill set manager for the given path
func resolveSkillSet(ctx context.Context, skillSetPath string) (interfaces.SkillSetManager, apperrors.Error) {
	if skillSetPath == "" {
		return nil, ErrInvalidObject.Msg("skillset path is required")
	}
	skillSetManager, err := catalogmanager.GetSkillSetManager(ctx, skillSetPath)
	if err != nil {
		return nil, err
	}
	return skillSetManager, nil
}

// Validate validates the session specification
func (s *SessionSpec) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors

	err := schemavalidator.V().Struct(s)
	if err == nil {
		if len(s.Variables) > 0 {
			var parsed any
			if err := json.Unmarshal(s.Variables, &parsed); err != nil {
				validationErrors = append(validationErrors, schemaerr.ErrValidationFailed("invalid session variables: "+err.Error()))
			} else {
				err = variableSchemaCompiled.Validate(parsed)
				if err != nil {
					msg := fmt.Sprintf("session variables must be key-value json objects with max 20 properties: %v", err)
					validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(msg))
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

func validateViewPolicy(ctx context.Context, viewDef *policy.ViewDefinition) apperrors.Error {
	ourViewDef := policy.GetViewDefinition(ctx)
	if ourViewDef == nil {
		return ErrInvalidView.Msg("no current view definition found")
	}

	if err := policy.ValidateDerivedView(ctx, ourViewDef, viewDef); err != nil {
		return err
	}

	return nil
}
