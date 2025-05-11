package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type ViewRuleEffect string

const (
	ViewRuleEffectAllow ViewRuleEffect = "Allow"
	ViewRuleEffectDeny  ViewRuleEffect = "Deny"
)

type ViewRuleAction string

const (
	ViewRuleActionRead    ViewRuleAction = "Read"
	ViewRuleActionWrite   ViewRuleAction = "Write"
	ViewRuleActionExecute ViewRuleAction = "Execute"
)

type ViewRule struct {
	Effect   ViewRuleEffect `json:"Effect" validate:"required,viewRuleEffectValidator"`
	Action   ViewRuleAction `json:"Action" validate:"required,viewRuleActionValidator"`
	Resource []string       `json:"Resource" validate:"required,dive,resourceURIValidator"`
}

type ViewRuleSet []ViewRule

// RulesFromJSON unmarshals a ViewRuleSet from a JSON byte slice.
func RulesFromJSON(data []byte) (ViewRuleSet, apperrors.Error) {
	var rules ViewRuleSet
	err := json.Unmarshal(data, &rules)
	if err != nil {
		return nil, ErrInvalidView.New("invalid ruleset")
	}
	return rules, nil
}

// ToJSON converts a ViewRuleSet to a JSON byte slice.
func (v ViewRuleSet) ToJSON() ([]byte, error) {
	return json.Marshal(v)
}

// viewSchema represents the structure of a view definition
type viewSchema struct {
	Version  string       `json:"version" validate:"required,requireVersionV1"`
	Kind     string       `json:"kind" validate:"required,kindValidator"`
	Metadata viewMetadata `json:"metadata" validate:"required"`
	Spec     viewSpec     `json:"spec" validate:"required"`
}

// viewMetadata contains metadata about a view
type viewMetadata struct {
	Name        string `json:"name" validate:"required,resourceNameValidator"`
	Catalog     string `json:"catalog" validate:"required,resourceNameValidator"`
	Description string `json:"description"`
}

// viewSpec contains the spec of a view
type viewSpec struct {
	Rules ViewRuleSet `json:"rules" validate:"required,dive"`
}

func (v *viewSchema) Validate() schemaerr.ValidationErrors {
	var validationErrors schemaerr.ValidationErrors
	if v.Kind != types.ViewKind {
		validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind("kind"))
	}
	err := schemavalidator.V().Struct(v)
	if err == nil {
		// Check for empty rules after struct validation
		if len(v.Spec.Rules) == 0 {
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute("spec.rules"))
		}
		return validationErrors
	}

	validatorErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(validationErrors, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(v).Elem()
	typeOfCS := value.Type()

	for _, e := range validatorErrors {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "resourceNameValidator":
			val, _ := e.Value().(string)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "kindValidator":
			validationErrors = append(validationErrors, schemaerr.ErrUnsupportedKind(jsonFieldName))
		case "requireVersionV1":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidVersion(jsonFieldName))
		case "resourceURIValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidResourceURI(jsonFieldName))
		case "viewRuleEffectValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidViewRuleEffect(jsonFieldName))
		case "viewRuleActionValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidViewRuleAction(jsonFieldName))
		default:
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	return validationErrors
}

// parseAndValidateView parses and validates a view from JSON bytes
func parseAndValidateView(resourceJSON []byte, catalog string) (*viewSchema, apperrors.Error) {
	view := &viewSchema{}
	if err := json.Unmarshal(resourceJSON, view); err != nil {
		return nil, ErrInvalidView.Msg("failed to parse view spec")
	}

	if catalog != "" {
		view.Metadata.Catalog = catalog
	}

	if err := view.Validate(); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	return view, nil
}

// resolveCatalogID resolves the catalog ID from context or by name
func resolveCatalogID(ctx context.Context, catalogName string) (uuid.UUID, apperrors.Error) {
	catalogID := common.GetCatalogIdFromContext(ctx)
	if catalogID == uuid.Nil {
		var err apperrors.Error
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, catalogName)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return uuid.Nil, ErrCatalogNotFound.New("catalog not found: " + catalogName)
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
			return uuid.Nil, err
		}
	}
	return catalogID, nil
}

// createViewModel creates a view model from a view schema and catalog ID
func createViewModel(view *viewSchema, catalogID uuid.UUID) (*models.View, apperrors.Error) {
	rulesJSON, err := view.Spec.Rules.ToJSON()
	if err != nil {
		return nil, ErrInvalidView.New("failed to marshal rules: " + err.Error())
	}

	return &models.View{
		Label:       view.Metadata.Name,
		Description: view.Metadata.Description,
		Info:        nil,
		Rules:       rulesJSON,
		CatalogID:   catalogID,
	}, nil
}

// CreateView creates a new view in the database
func CreateView(ctx context.Context, resourceJSON []byte, catalog string) apperrors.Error {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return ErrInvalidProject
	}

	view, err := parseAndValidateView(resourceJSON, catalog)
	if err != nil {
		return err
	}

	catalogID, err := resolveCatalogID(ctx, view.Metadata.Catalog)
	if err != nil {
		return err
	}

	v, err := createViewModel(view, catalogID)
	if err != nil {
		return err
	}

	if err := db.DB(ctx).CreateView(ctx, v); err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.New("view already exists: " + view.Metadata.Name)
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create view")
		return ErrCatalogError.New("failed to create view: " + err.Error())
	}

	return nil
}

// UpdateView updates an existing view in the database
func UpdateView(ctx context.Context, resourceJSON []byte, catalog string) apperrors.Error {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return ErrInvalidProject
	}

	view, err := parseAndValidateView(resourceJSON, catalog)
	if err != nil {
		return err
	}

	catalogID, err := resolveCatalogID(ctx, view.Metadata.Catalog)
	if err != nil {
		return err
	}

	v, err := createViewModel(view, catalogID)
	if err != nil {
		return err
	}

	if err := db.DB(ctx).UpdateView(ctx, v); err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrViewNotFound.New("view not found: " + view.Metadata.Name)
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update view")
		return ErrCatalogError.New("failed to update view: " + err.Error())
	}

	return nil
}

// viewRuleEffectValidator validates that the effect is one of the allowed values
func viewRuleEffectValidator(fl validator.FieldLevel) bool {
	effect := ViewRuleEffect(fl.Field().String())
	return effect == ViewRuleEffectAllow || effect == ViewRuleEffectDeny
}

// viewRuleActionValidator validates that the action is one of the allowed values
func viewRuleActionValidator(fl validator.FieldLevel) bool {
	action := ViewRuleAction(fl.Field().String())
	return action == ViewRuleActionRead || action == ViewRuleActionWrite || action == ViewRuleActionExecute
}

func init() {
	validate := schemavalidator.V()
	validate.RegisterValidation("viewRuleEffectValidator", viewRuleEffectValidator)
	validate.RegisterValidation("viewRuleActionValidator", viewRuleActionValidator)
}
