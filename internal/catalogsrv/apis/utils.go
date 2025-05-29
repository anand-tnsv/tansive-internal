package apis

import (
	"net/http"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/interfaces"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tidwall/gjson"
)

func hydrateRequestContext(r *http.Request) (interfaces.RequestContext, error) {
	ctx := r.Context()
	viewName := chi.URLParam(r, "viewName")
	resourcePath := chi.URLParam(r, "resourcePath")
	resourceValue := chi.URLParam(r, "resourceValue")

	n := interfaces.RequestContext{}

	catalogCtx := catcommon.GetCatalogContext(ctx)
	if catalogCtx != nil {
		n.Catalog = catalogCtx.Catalog
		n.CatalogID = catalogCtx.CatalogID
		n.Variant = catalogCtx.Variant
		n.VariantID = catalogCtx.VariantID
		n.Namespace = catalogCtx.Namespace
	} else {
		log.Ctx(ctx).Error().Msg("no catalog context found")
	}

	if viewName != "" {
		n.ObjectName = viewName
	}

	if resourcePath != "" {
		n.ObjectName = path.Base(resourcePath)
		if n.ObjectName == "/" || n.ObjectName == "." {
			n.ObjectName = ""
		}
		n.ObjectPath = path.Dir(resourcePath)
		if n.ObjectPath == "." {
			n.ObjectPath = "/"
		}
		n.ObjectPath = path.Clean("/" + n.ObjectPath)
		n.ObjectType = catcommon.CatalogObjectTypeResource
		n.ObjectProperty = catcommon.ResourcePropertyDefinition
	}

	if resourceValue != "" {
		n.ObjectName = path.Base(resourceValue)
		if n.ObjectName == "/" || n.ObjectName == "." {
			n.ObjectName = ""
		}
		n.ObjectPath = path.Dir(resourceValue)
		if n.ObjectPath == "." {
			n.ObjectPath = "/"
		}
		n.ObjectPath = path.Clean("/" + n.ObjectPath)
		n.ObjectType = catcommon.CatalogObjectTypeResource
		n.ObjectProperty = catcommon.ResourcePropertyValue
	}

	n.QueryParams = r.URL.Query()

	return n, nil
}

func getResourceKind(r *http.Request) string {
	return catcommon.KindFromKindName(getResourceNameFromPath(r))
}

func getResourceNameFromPath(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.Split(path, "/")
	var resourceName string
	if len(segments) > 0 {
		resourceName = segments[0]
	}
	return resourceName
}

func validateRequest(reqJSON []byte, kind string) error {
	if !gjson.ValidBytes(reqJSON) {
		return httpx.ErrInvalidRequest("unable to parse request")
	}
	if kind == catcommon.ResourceKind {
		return nil
	}
	result := gjson.GetBytes(reqJSON, "kind")
	if !result.Exists() {
		return httpx.ErrInvalidRequest("missing kind")
	}
	if result.String() != kind {
		return httpx.ErrInvalidRequest("invalid kind")
	}
	return nil
}
