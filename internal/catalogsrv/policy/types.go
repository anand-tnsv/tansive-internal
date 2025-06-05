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
	ActionSkillSetAdmin     Action = "skillset.admin"
	ActionSkillSetCreate    Action = "skillset.create"
	ActionSkillSetRead      Action = "skillset.read"
	ActionSkillSetEdit      Action = "skillset.edit"
	ActionSkillSetDelete    Action = "skillset.delete"
	ActionSkillSetList      Action = "skillset.list"
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
	ActionSkillSetCreate,
	ActionSkillSetRead,
	ActionSkillSetEdit,
	ActionSkillSetDelete,
	ActionSkillSetList,
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

func (v ViewDefinition) DeepCopy() ViewDefinition {
	return ViewDefinition{
		Scope: v.Scope, // Scope is a struct of strings (safe to copy)
		Rules: v.Rules.DeepCopy(),
	}
}

func (r Rules) DeepCopy() Rules {
	copied := make(Rules, len(r))
	for i, rule := range r {
		copied[i] = rule.DeepCopy()
	}
	return copied
}

func (r Rule) DeepCopy() Rule {
	actionsCopy := make([]Action, len(r.Actions))
	copy(actionsCopy, r.Actions)

	targetsCopy := make([]TargetResource, len(r.Targets))
	copy(targetsCopy, r.Targets)

	return Rule{
		Intent:  r.Intent,
		Actions: actionsCopy,
		Targets: targetsCopy,
	}
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
