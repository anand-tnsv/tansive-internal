package tangent

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tansive/tansive-internal/internal/catalogsrv/apis"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

var tangentHandlers = []policy.ResponseHandlerParam{
	{
		Method: http.MethodPost,
		Path:   "/",
		//		Handler: createTangent,
	},
}

var tangentUserHandlers = []policy.ResponseHandlerParam{
	{
		Method: http.MethodGet,
		Path:   "/onboardingKey",
		//		Handler:        getOnboardingKey,
		AllowedActions: []policy.Action{policy.ActionTangentCreate},
	},
	// {
	// 	Method: http.MethodPut,
	// 	Path:   "/catalogs/{catalogName}",
	// 	//		Handler:        updateObject,
	// 	AllowedActions: []policy.Action{policy.ActionCatalogAdmin},
	// },
	// {
	// 	Method: http.MethodDelete,
	// 	Path:   "/catalogs/{catalogName}",
	// 	//Handler:        deleteObject,
	// 	AllowedActions: []policy.Action{policy.ActionCatalogAdmin},
	// },
}

func Router() chi.Router {
	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(auth.ContextMiddleware)
		r.Use(apis.CatalogContextLoader)
		for _, handler := range tangentUserHandlers {
			policyEnforcedHandler := policy.EnforceViewPolicyMiddleware(handler)
			r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(policyEnforcedHandler))
		}
	})
	for _, handler := range tangentHandlers {
		r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
	return r
}
