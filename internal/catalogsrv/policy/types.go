package policy

import (
	json "github.com/json-iterator/go"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

type Intent string

const (
	IntentAllow Intent = "Allow"
	IntentDeny  Intent = "Deny"
)

type Action string

const (
	ActionCatalogAdmin      Action = "catalog.admin"
	ActionCatalogList       Action = "catalog.list"
	ActionCatalogAdoptView  Action = "catalog.adoptView"
	ActionCatalogCreateView Action = "catalog.createView"
	ActionViewAdmin         Action = "view.admin"
	ActionVariantAdmin      Action = "variant.admin"
	ActionVariantClone      Action = "variant.clone"
	ActionVariantList       Action = "variant.list"
	ActionNamespaceCreate   Action = "namespace.create"
	ActionNamespaceList     Action = "namespace.list"
	ActionNamespaceAdmin    Action = "namespace.admin"
	ActionResourceCreate    Action = "resource.create"
	ActionResourceRead      Action = "resource.read"
	ActionResourceEdit      Action = "resource.edit"
	ActionResourceDelete    Action = "resource.delete"
	ActionResourceGet       Action = "resource.get"
	ActionResourcePut       Action = "resource.put"
	ActionResourceList      Action = "resource.list"
)

var ValidActions = []Action{
	ActionCatalogAdmin,
	ActionCatalogList,
	ActionCatalogAdoptView,
	ActionCatalogCreateView,
	ActionVariantAdmin,
	ActionVariantClone,
	ActionVariantList,
	ActionNamespaceCreate,
	ActionNamespaceList,
	ActionNamespaceAdmin,
	ActionResourceCreate,
	ActionResourceRead,
	ActionResourceEdit,
	ActionResourceDelete,
	ActionResourceGet,
	ActionResourcePut,
	ActionResourceList,
}

type Rule struct {
	Intent  Intent           `json:"intent" validate:"required,viewRuleIntentValidator"`
	Actions []Action         `json:"actions" validate:"required,dive,viewRuleActionValidator"`
	Targets []TargetResource `json:"targets" validate:"-"`
}

type TargetResource string
type Rules []Rule
type Scope struct {
	Catalog   string `json:"catalog"`
	Variant   string `json:"variant"`
	Namespace string `json:"namespace"`
}

func (v Scope) Equals(other Scope) bool {
	return v.Catalog == other.Catalog &&
		v.Variant == other.Variant &&
		v.Namespace == other.Namespace
}

type ViewDefinition struct {
	Scope Scope `json:"scope" validate:"required"`
	Rules Rules `json:"rules" validate:"required,dive"`
}

// ToJSON converts a ViewRuleSet to a JSON byte slice.
func (v ViewDefinition) ToJSON() ([]byte, error) {
	return json.Marshal(v)
}

type ResponseHandlerParam struct {
	Method         string
	Path           string
	Handler        httpx.RequestHandler
	AllowedActions []Action
}
