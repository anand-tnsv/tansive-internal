package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"

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
	Definition types.ViewDefinition `json:"definition" validate:"required"`
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
		if len(v.Spec.Definition.Rules) == 0 {
			validationErrors = append(validationErrors, schemaerr.ErrMissingRequiredAttribute("spec.rules"))
		}
		for _, rule := range v.Spec.Definition.Rules {
			if len(rule.Targets) == 0 {
				m := MorphResource(v.Spec.Definition.Scope, "")
				if m == "" || schemavalidator.V().Var(m, "resourceURIValidator") != nil {
					validationErrors = append(validationErrors, schemaerr.ErrInvalidResourceURI("null"))
				}
			}
			for _, res := range rule.Targets {
				m := MorphResource(v.Spec.Definition.Scope, res)
				if m == "" || schemavalidator.V().Var(m, "resourceURIValidator") != nil {
					validationErrors = append(validationErrors, schemaerr.ErrInvalidResourceURI(string(res)))
				}
			}
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
		case "viewRuleIntentValidator":
			validationErrors = append(validationErrors, schemaerr.ErrInvalidViewRuleIntent(jsonFieldName))
		case "viewRuleActionValidator":
			fieldName, _ := e.Value().(types.Action)
			validationErrors = append(validationErrors, schemaerr.ErrInvalidViewRuleAction(string(fieldName)))
		default:
			validationErrors = append(validationErrors, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	return validationErrors
}

// parseAndValidateView parses a JSON byte slice into a viewSchema, validates it,
// and optionally overrides the name and catalog fields.
// Returns an error if the JSON is invalid or the schema validation fails.
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
	view.Spec.Definition.Scope.Catalog = view.Metadata.Catalog // ensure catalog in scope the same as the view
	rulesJSON, err := view.Spec.Definition.ToJSON()
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

// removeDuplicates removes duplicate elements from a slice while preserving order.
// Returns a new slice containing only unique elements in their original order.
func removeDuplicates[T comparable](slice []T) []T {
	if len(slice) == 0 {
		return slice
	}

	seen := make(map[T]struct{}, len(slice))
	unique := make([]T, 0, len(slice))

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

// deduplicateRules removes duplicate actions and targets from each rule in the ViewRuleSet.
// Returns a new ViewRuleSet with all duplicates removed while preserving the original order.
func deduplicateRules(rules types.Rules) types.Rules {
	if len(rules) == 0 {
		return rules
	}

	result := make(types.Rules, len(rules))
	for i, rule := range rules {
		result[i] = types.Rule{
			Intent:  rule.Intent,
			Actions: removeDuplicates(rule.Actions),
			Targets: removeDuplicates(rule.Targets),
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
	view.Spec.Definition.Rules = deduplicateRules(view.Spec.Definition.Rules)

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
	view.Spec.Definition.Rules = deduplicateRules(view.Spec.Definition.Rules)

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
	var definition types.ViewDefinition
	if err := json.Unmarshal(view.Rules, &definition); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal view rules")
		return nil, ErrUnableToLoadObject.Msg("unable to unmarshal view rules")
	}
	viewSchema.Spec.Definition = definition

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

	err := db.DB(ctx).DeleteViewByLabel(ctx, vr.reqCtx.ObjectName, vr.reqCtx.CatalogID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil
		}
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

func (vr *viewResource) List(ctx context.Context) ([]byte, apperrors.Error) {
	if vr.reqCtx.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}

	views, err := db.DB(ctx).ListViewsByCatalog(ctx, vr.reqCtx.CatalogID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load views")
		return nil, ErrUnableToLoadObject.Msg("unable to load view")
	}

	type viewItem struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	type viewListRsp struct {
		Views []viewItem `json:"views"`
	}

	viewsRsp := viewListRsp{
		Views: make([]viewItem, len(views)),
	}
	for i, v := range views {
		if strings.HasPrefix(v.Label, "_") {
			continue
		}
		viewsRsp.Views[i] = viewItem{
			Name:        v.Label,
			Description: v.Description,
		}
	}

	jsonData, e := json.Marshal(viewsRsp)
	if e != nil {
		log.Ctx(ctx).Error().Err(e).Msg("failed to marshal view list")
		return nil, ErrUnableToLoadObject.Msg("unable to marshal view list")
	}

	return jsonData, nil
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

// ValidateDerivedView ensures that a derived view is valid with respect to its parent view.
// It ensures that the derived view's scope is the same as the parent's and that all rules in the derived view
// are permissible by the parent view.
func ValidateDerivedView(ctx context.Context, parent *types.ViewDefinition, child *types.ViewDefinition) apperrors.Error {
	if parent == nil || child == nil {
		return ErrInvalidView
	}

	if !parent.Scope.Equals(child.Scope) {
		return ErrInvalidView.New("derived view scope must match parent view scope")
	}

	if !child.Rules.IsSubsetOf(parent.Rules) {
		return ErrInvalidView.New("derived view rules must be a subset of parent view rules")
	}

	return nil
}

// MorphResource transforms a resource string based on the provided scope.
// It handles the conversion of resource paths and ensures proper formatting
// of catalog, variant, workspace, and namespace components.
func MorphResource(scope types.Scope, resource types.TargetResource) types.TargetResource {
	segments, resourceName, err := extractSegmentsAndResourceName(resource)
	if err != nil && len(resource) > 0 {
		return ""
	}

	var metadata string
	if len(segments) > 0 {
		metadata = string(segments[0])
	}

	resourceKV := extractKV(metadata)

	var morphedMetadata = make(map[string]resourceMetadataValue)

	morphedMetadata[types.ResourceNameCatalogs] = morphMetadata(scope.Catalog, 0, types.ResourceNameCatalogs, resourceKV)
	morphedMetadata[types.ResourceNameVariants] = morphMetadata(scope.Variant, 1, types.ResourceNameVariants, resourceKV)
	morphedMetadata[types.ResourceNameWorkspaces] = morphMetadata(scope.Workspace, 2, types.ResourceNameWorkspaces, resourceKV)
	morphedMetadata[types.ResourceNameNamespaces] = morphMetadata(scope.Namespace, 3, types.ResourceNameNamespaces, resourceKV)

	s := strings.Builder{}
	s.WriteString(types.ResourceNameCatalogs + "/" + morphedMetadata[types.ResourceNameCatalogs].value)
	if morphedMetadata[types.ResourceNameVariants].value != "" {
		s.WriteString("/" + types.ResourceNameVariants + "/" + morphedMetadata[types.ResourceNameVariants].value)
	}
	if morphedMetadata[types.ResourceNameWorkspaces].value != "" {
		s.WriteString("/" + types.ResourceNameWorkspaces + "/" + morphedMetadata[types.ResourceNameWorkspaces].value)
	}
	if morphedMetadata[types.ResourceNameNamespaces].value != "" {
		s.WriteString("/" + types.ResourceNameNamespaces + "/" + morphedMetadata[types.ResourceNameNamespaces].value)
	}

	// write remaining segments in metadata in sorted order. Usually this will end up as erroneous segments
	// but, this will be caught by the validator and result in a meaningful error message.
	type kv struct {
		key   string
		value resourceMetadataValue
	}
	var sorted []kv = make([]kv, len(resourceKV))
	for k, v := range resourceKV {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].value.pos < sorted[j].value.pos
	})
	for _, v := range sorted {
		if v.key == "" {
			continue
		}
		if v.value.value == "" {
			s.WriteString("/" + v.key)
		} else {
			s.WriteString("/" + v.key + "/" + v.value.value)
		}
	}

	if resourceName != "" {
		resourceName = strings.TrimPrefix(resourceName, "/")
		s.WriteString("/" + resourceName)
	}
	if len(segments) > 1 {
		path := strings.TrimPrefix(string(segments[1]), "/")
		segments[1] = types.TargetResource(path)
		s.WriteString("/" + path)
	}

	return types.TargetResource("res://" + s.String())
}

// morphMetadata processes a single metadata field based on scope name and resource type.
// Returns a resourceMetadataValue with the appropriate position and value.
func morphMetadata(scopeName string, pos int, resourceType string, resourceMetadata map[string]resourceMetadataValue) resourceMetadataValue {
	m := resourceMetadataValue{}
	m.pos = pos
	if scopeName == "" {
		if v, ok := resourceMetadata[resourceType]; ok {
			m.value = v.value
			if v.value == "" {
				return m
			}
		}
	} else {
		m.value = scopeName
	}
	delete(resourceMetadata, resourceType)
	return m
}

// morph all targets in the view definition to its scope
func MorphViewDefinition(vd *types.ViewDefinition) *types.ViewDefinition {
	if vd == nil {
		return nil
	}
	for i, rule := range vd.Rules {
		for j, target := range rule.Targets {
			vd.Rules[i].Targets[j] = types.TargetResource(MorphResource(vd.Scope, target))
		}
	}
	return vd
}
