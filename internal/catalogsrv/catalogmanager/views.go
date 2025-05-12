package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type Intent string

const (
	ViewRuleEffectAllow Intent = "Allow"
	ViewRuleEffectDeny  Intent = "Deny"
)

type Operation string

var validOperations = []Operation{
	OpCatalogAdmin,
	OpCatalogList,
	OpVariantAdmin,
	OpVariantClone,
	OpVariantCreateView,
	OpVariantList,
	OpNamespaceCreate,
	OpNamespaceEdit,
	OpNamespaceList,
	OpNamespaceAdmin,
	OpSchemaCreate,
	OpSchemaRead,
	OpSchemaEdit,
	OpSchemaDelete,
	OpSchemaAssign,
	OpCollectionCreate,
	OpCollectionRead,
	OpCollectionWrite,
	OpCollectionRun,
	OpWorkspaceAdmin,
	OpWorkspaceList,
	OpWorkspaceCreate,
}

const (
	OpCatalogAdmin      Operation = "catalog.admin"
	OpCatalogList       Operation = "catalog.list"
	OpVariantAdmin      Operation = "variant.admin"
	OpVariantClone      Operation = "variant.clone"
	OpVariantCreateView Operation = "variant.createView"
	OpVariantList       Operation = "variant.list"
	OpNamespaceCreate   Operation = "namespace.create"
	OpNamespaceEdit     Operation = "namespace.edit"
	OpNamespaceList     Operation = "namespace.list"
	OpNamespaceAdmin    Operation = "namespace.admin"
	OpSchemaCreate      Operation = "schema.create"
	OpSchemaRead        Operation = "schema.read"
	OpSchemaEdit        Operation = "schema.edit"
	OpSchemaDelete      Operation = "schema.delete"
	OpSchemaAssign      Operation = "schema.assign"
	OpCollectionCreate  Operation = "collection.create"
	OpCollectionRead    Operation = "collection.read"
	OpCollectionWrite   Operation = "collection.write"
	OpCollectionRun     Operation = "collection.run"
	OpWorkspaceAdmin    Operation = "workspace.admin"
	OpWorkspaceList     Operation = "workspace.list"
	OpWorkspaceCreate   Operation = "workspace.create"
)

type AccessRule struct {
	Intent    Intent           `json:"Intent" validate:"required,viewRuleEffectValidator"`
	Operation []Operation      `json:"Operation" validate:"required,dive,viewRuleActionValidator"`
	Target    []TargetResource `json:"Target" validate:"required,dive,resourceURIValidator"`
}

type TargetResource string
type AccessRuleSet []AccessRule

// RulesFromJSON unmarshals a ViewRuleSet from a JSON byte slice.
func RulesFromJSON(data []byte) (AccessRuleSet, apperrors.Error) {
	var rules AccessRuleSet
	err := json.Unmarshal(data, &rules)
	if err != nil {
		return nil, ErrInvalidView.New("invalid ruleset")
	}
	return rules, nil
}

// ToJSON converts a ViewRuleSet to a JSON byte slice.
func (v AccessRuleSet) ToJSON() ([]byte, error) {
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
	Rules AccessRuleSet `json:"rules" validate:"required,dive"`
}

// Validate performs validation on the view schema and returns any validation errors.
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

// parseAndValidateView parses and validates a view from JSON bytes, optionally overriding the name and catalog.
func parseAndValidateView(resourceJSON []byte, viewName string, catalog string) (*viewSchema, apperrors.Error) {
	view := &viewSchema{}
	if err := json.Unmarshal(resourceJSON, view); err != nil {
		return nil, ErrInvalidView.Msg("failed to parse view spec")
	}

	if catalog != "" {
		view.Metadata.Catalog = catalog
	}

	if viewName != "" {
		view.Metadata.Name = viewName
	}

	if err := view.Validate(); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	return view, nil
}

// resolveCatalogID resolves the catalog ID from context or by name.
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

// createViewModel creates a view model from a view schema and catalog ID.
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

// removeDuplicates removes duplicate values from a slice of any comparable type.
// It preserves the original order of elements while efficiently removing duplicates.
func removeDuplicates[T comparable](slice []T) []T {
	if len(slice) == 0 {
		return slice
	}

	// Pre-allocate the map with the expected size
	seen := make(map[T]struct{}, len(slice))
	// Pre-allocate the result slice with the maximum possible size
	unique := make([]T, 0, len(slice))

	// Use struct{} instead of bool for minimal memory usage
	for _, v := range slice {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			unique = append(unique, v)
		}
	}

	// If no duplicates were found, return the original slice
	if len(unique) == len(slice) {
		return slice
	}

	return unique
}

// deduplicateRules removes duplicates from both actions and resources in a ViewRuleSet.
// It returns a new ViewRuleSet with duplicates removed.
func deduplicateRules(rules AccessRuleSet) AccessRuleSet {
	if len(rules) == 0 {
		return rules
	}

	result := make(AccessRuleSet, len(rules))
	for i, rule := range rules {
		result[i] = AccessRule{
			Intent:    rule.Intent,
			Operation: removeDuplicates(rule.Operation),
			Target:    removeDuplicates(rule.Target),
		}
	}
	return result
}

// CreateView creates a new view in the database.
func CreateView(ctx context.Context, resourceJSON []byte, catalog string) (*models.View, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	view, err := parseAndValidateView(resourceJSON, "", catalog)
	if err != nil {
		return nil, err
	}

	// Remove duplicates from rules
	view.Spec.Rules = deduplicateRules(view.Spec.Rules)

	catalogID, err := resolveCatalogID(ctx, view.Metadata.Catalog)
	if err != nil {
		return nil, err
	}

	v, err := createViewModel(view, catalogID)
	if err != nil {
		return nil, err
	}

	if err := db.DB(ctx).CreateView(ctx, v); err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return nil, ErrAlreadyExists.New("view already exists: " + view.Metadata.Name)
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create view")
		return nil, ErrCatalogError.New("failed to create view: " + err.Error())
	}

	return v, nil
}

// UpdateView updates an existing view in the database.
func UpdateView(ctx context.Context, resourceJSON []byte, viewName string, catalog string) (*models.View, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	view, err := parseAndValidateView(resourceJSON, viewName, catalog)
	if err != nil {
		return nil, err
	}

	// Remove duplicates from rules
	view.Spec.Rules = deduplicateRules(view.Spec.Rules)

	catalogID, err := resolveCatalogID(ctx, view.Metadata.Catalog)
	if err != nil {
		return nil, err
	}

	v, err := createViewModel(view, catalogID)
	if err != nil {
		return nil, err
	}

	if err := db.DB(ctx).UpdateView(ctx, v); err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrViewNotFound.New("view not found: " + view.Metadata.Name)
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update view")
		return nil, ErrCatalogError.New("failed to update view: " + err.Error())
	}

	return v, nil
}

type viewResource struct {
	reqCtx RequestContext
	view   *models.View
}

// Name returns the name of the view resource.
func (vr *viewResource) Name() string {
	return vr.reqCtx.ObjectName
}

// Location returns the location path of the view resource.
func (vr *viewResource) Location() string {
	return "/views/" + vr.view.Label
}

// Create creates a new view resource.
func (vr *viewResource) Create(ctx context.Context, resourceJSON []byte) (string, apperrors.Error) {
	v, err := CreateView(ctx, resourceJSON, vr.reqCtx.Catalog)
	if err != nil {
		return "", err
	}
	vr.view = v
	return vr.Location(), nil
}

// Get retrieves a view resource by its name.
func (vr *viewResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	if vr.reqCtx.CatalogID == uuid.Nil || vr.reqCtx.ObjectName == "" {
		return nil, ErrInvalidView
	}

	view, err := db.DB(ctx).GetViewByLabel(ctx, vr.reqCtx.ObjectName, vr.reqCtx.CatalogID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrViewNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load view")
		return nil, ErrUnableToLoadObject.Msg("unable to load view")
	}

	vr.view = view

	// Convert the view model to JSON
	viewSchema := &viewSchema{
		Version: types.VersionV1,
		Kind:    types.ViewKind,
		Metadata: viewMetadata{
			Name:        view.Label,
			Catalog:     vr.reqCtx.Catalog,
			Description: view.Description,
		},
	}

	// Parse the rules from the view model
	var rules AccessRuleSet
	if err := json.Unmarshal(view.Rules, &rules); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal view rules")
		return nil, ErrUnableToLoadObject.Msg("unable to unmarshal view rules")
	}
	viewSchema.Spec.Rules = rules

	jsonData, e := json.Marshal(viewSchema)
	if e != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal view schema")
		return nil, ErrUnableToLoadObject.Msg("unable to fetch view schema")
	}

	return jsonData, nil
}

// Delete removes a view resource.
func (vr *viewResource) Delete(ctx context.Context) apperrors.Error {
	if vr.reqCtx.CatalogID == uuid.Nil || vr.reqCtx.ObjectName == "" {
		return ErrInvalidView
	}

	// First get the view to get its ID
	view, err := db.DB(ctx).GetViewByLabel(ctx, vr.reqCtx.ObjectName, vr.reqCtx.CatalogID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load view")
		return ErrUnableToLoadObject.Msg("unable to load view")
	}

	// Delete using the view ID
	err = db.DB(ctx).DeleteView(ctx, view.ViewID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete view")
		return ErrUnableToDeleteObject.Msg("unable to delete view")
	}

	return nil
}

// Update modifies an existing view resource.
func (vr *viewResource) Update(ctx context.Context, resourceJSON []byte) apperrors.Error {
	v, err := UpdateView(ctx, resourceJSON, vr.reqCtx.ObjectName, vr.reqCtx.Catalog)
	if err != nil {
		return err
	}
	vr.view = v
	return nil
}

// NewViewResource creates a new view resource manager.
func NewViewResource(ctx context.Context, reqCtx RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if reqCtx.Catalog == "" || reqCtx.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	return &viewResource{
		reqCtx: reqCtx,
	}, nil
}

// IsActionAllowed checks if a given action is allowed for a specific resource based on the rule set.
// It returns true if the action is allowed, false otherwise. Deny rules take precedence over allow rules.
func (ruleSet AccessRuleSet) IsActionAllowed(action Operation, resource string) bool {
	allowMatch := false
	// check if there is an admin match
	if ruleSet.matchesAdmin(resource) {
		allowMatch = true
	}
	// check if there is a match for the action
	for _, rule := range ruleSet {
		if slices.Contains(rule.Operation, action) {
			for _, res := range rule.Target {
				if res.matches(TargetResource(resource)) {
					if rule.Intent == ViewRuleEffectAllow {
						allowMatch = true
					} else if rule.Intent == ViewRuleEffectDeny {
						allowMatch = false
					}
				}
			}
		}
	}
	return allowMatch
}
