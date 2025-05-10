package apis

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/pkg/types"
)

var resourceObjectHandlers = []httpx.ResponseHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/catalogs",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/catalogs/{catalogName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/catalogs/{catalogName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/catalogs/{catalogName}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/variants",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/variants/{variantName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/variants/{variantName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/variants/{variantName}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/workspaces",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/workspaces/{workspaceRef}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/workspaces/{workspaceRef}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/workspaces/{workspaceRef}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/namespaces",
		Handler: createObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/namespaces/{namespaceName}",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/namespaces/{namespaceName}",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/namespaces/{namespaceName}",
		Handler: deleteObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/{objectType}",
		Handler: createObject,
	},
	{
		Method:  http.MethodPost,
		Path:    "/{objectType}/*",
		Handler: updateObject,
	},
	{
		Method:  http.MethodGet,
		Path:    "/{objectType}/*",
		Handler: getObject,
	},
	{
		Method:  http.MethodPut,
		Path:    "/{objectType}/*",
		Handler: updateObject,
	},
	{
		Method:  http.MethodDelete,
		Path:    "/{objectType}/*",
		Handler: deleteObject,
	},
}

func Router(r chi.Router) {
	r.Use(CreateTenantProject) // Only for testing
	r.Use(LoadCatalogContext)
	//TODO: Implement authentication
	for _, handler := range resourceObjectHandlers {
		r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
}

func CreateTenantProject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantID := common.TenantIdFromContext(ctx)
		projectID := common.ProjectIdFromContext(ctx)
		if tenantID != "" && projectID != "" {
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// get the bearer token from the request
		bearerToken := r.Header.Get("Authorization")
		if bearerToken == "" {
			httpx.ErrUnAuthorized().Send(w)
			return
		}
		// this is only for testing.
		tenantID = types.TenantId(bearerToken[strings.LastIndex(bearerToken, "-")+1:])
		projectID = types.ProjectId("P" + tenantID)

		// Create the tenant for testing
		err := db.DB(ctx).CreateTenant(ctx, types.TenantId(tenantID))
		if err == nil {
			log.Ctx(ctx).Info().Msg("Tenant created successfully")
		} else {
			log.Ctx(ctx).Error().Msgf("Tenant creation failed: %v", err)
		}

		// Set the tenant ID in the context
		ctx = common.SetTenantIdInContext(ctx, types.TenantId(tenantID))

		// Create the project for testing
		err = db.DB(ctx).CreateProject(ctx, types.ProjectId(projectID))
		if err == nil {
			log.Ctx(ctx).Info().Msg("Project created successfully")
		} else {
			log.Ctx(ctx).Error().Msgf("Project creation failed: %v", err)
		}

		ctx = common.SetProjectIdInContext(ctx, types.ProjectId(projectID))

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func LoadCatalogContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantId := common.TenantIdFromContext(ctx)
		projectId := common.ProjectIdFromContext(ctx)
		if tenantId == "" || projectId == "" {
			httpx.ErrInvalidRequest().Send(w)
			return
		}
		c := common.CatalogContextFromContext(ctx)
		if c == nil {
			// TODO: return here, since this will be a breach of token
			c = &common.CatalogContext{}
			ctx = common.SetCatalogContext(ctx, c)
			//next.ServeHTTP(w, r.WithContext(ctx))
			//return
		}
		urlValues := r.URL.Query()
		// Load Catalog
		c, err := loadCatalogObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidCatalog().Send(w)
			return
		}

		// Load Variant
		c, err = loadVariantObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidVariant().Send(w)
			return
		}
		// Load Workspace
		c, err = loadWorkspaceObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidWorkspace().Send(w)
			return
		}

		// Load Namespace
		c, err = loadNamespaceObject(ctx, c, urlValues)
		if err != nil {
			httpx.ErrInvalidNamespace().Send(w)
			return
		}

		ctx = common.SetCatalogContext(ctx, c)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
