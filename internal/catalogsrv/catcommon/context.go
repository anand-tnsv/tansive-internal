// Package catcommon provides context management utilities for the catalog service.
// It includes functionality for managing tenant, project, catalog, and user contexts.
package catcommon

import (
	"context"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

// ctxKeyType represents the type for all context keys
type ctxKeyType string

// Context keys for different types of data
const (
	// Catalog related keys
	ctxCatalogContextKey ctxKeyType = "CatalogContext"
	ctxTenantIdKey       ctxKeyType = "CatalogTenantId"
	ctxProjectIdKey      ctxKeyType = "CatalogProjectId"
	ctxTestContextKey    ctxKeyType = "CatalogTestContext"
)

// CatalogContext represents the complete context for catalog operations.
// It contains all necessary information about the catalog, variant, and user.
type CatalogContext struct {
	// CatalogId is the unique identifier for the catalog
	CatalogId uuid.UUID
	// VariantId is the unique identifier for the variant
	VariantId uuid.UUID
	// Namespace is the namespace for the catalog
	Namespace string
	// Catalog is the name of the catalog
	Catalog string
	// Variant is the name of the variant
	Variant string
	// ViewDefinition contains the view definition for the catalog
	ViewDefinition *types.ViewDefinition
	// UserContext contains information about the authenticated user
	UserContext *UserContext
}

// UserContext represents the context of an authenticated user in the system.
// It contains information about the user's identity and permissions.
type UserContext struct {
	// UserID is the unique identifier for the user
	UserID string
}

// SetTenantIdInContext sets the tenant ID in the provided context.
func SetTenantIdInContext(ctx context.Context, tenantId types.TenantId) context.Context {
	return context.WithValue(ctx, ctxTenantIdKey, tenantId)
}

// TenantIdFromContext retrieves the tenant ID from the provided context.
func TenantIdFromContext(ctx context.Context) types.TenantId {
	if tenantId, ok := ctx.Value(ctxTenantIdKey).(types.TenantId); ok {
		return tenantId
	}
	return ""
}

// SetProjectIdInContext sets the project ID in the provided context.
func SetProjectIdInContext(ctx context.Context, projectId types.ProjectId) context.Context {
	return context.WithValue(ctx, ctxProjectIdKey, projectId)
}

// ProjectIdFromContext retrieves the project ID from the provided context.
func ProjectIdFromContext(ctx context.Context) types.ProjectId {
	if projectId, ok := ctx.Value(ctxProjectIdKey).(types.ProjectId); ok {
		return projectId
	}
	return ""
}

// SetCatalogContext sets the catalog context in the provided context.
func SetCatalogContext(ctx context.Context, catalogContext *CatalogContext) context.Context {
	return context.WithValue(ctx, ctxCatalogContextKey, catalogContext)
}

// CatalogContextFromContext retrieves the catalog context from the provided context.
func CatalogContextFromContext(ctx context.Context) *CatalogContext {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext
	}
	return nil
}

// SetCatalogIdInContext sets the catalog ID in the provided context.
func SetCatalogIdInContext(ctx context.Context, catalogId uuid.UUID) context.Context {
	currContext := CatalogContextFromContext(ctx)
	if currContext == nil {
		currContext = &CatalogContext{}
	}
	currContext.CatalogId = catalogId
	return SetCatalogContext(ctx, currContext)
}

// SetVariantIdInContext sets the variant ID in the provided context.
func SetVariantIdInContext(ctx context.Context, variantId uuid.UUID) context.Context {
	currContext := CatalogContextFromContext(ctx)
	if currContext == nil {
		currContext = &CatalogContext{}
	}
	currContext.VariantId = variantId
	return SetCatalogContext(ctx, currContext)
}

// SetNamespaceInContext sets the namespace in the provided context.
func SetNamespaceInContext(ctx context.Context, namespace string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	if currContext == nil {
		currContext = &CatalogContext{}
	}
	currContext.Namespace = namespace
	return SetCatalogContext(ctx, currContext)
}

// SetCatalogInContext sets the catalog in the provided context.
func SetCatalogInContext(ctx context.Context, catalog string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	if currContext == nil {
		currContext = &CatalogContext{}
	}
	currContext.Catalog = catalog
	return SetCatalogContext(ctx, currContext)
}

// SetVariantInContext sets the variant in the provided context.
func SetVariantInContext(ctx context.Context, variant string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	if currContext == nil {
		currContext = &CatalogContext{}
	}
	currContext.Variant = variant
	return SetCatalogContext(ctx, currContext)
}

// SetViewDefinitionInContext sets the view definition in the provided context.
func SetViewDefinitionInContext(ctx context.Context, viewDefinition *types.ViewDefinition) context.Context {
	currContext := CatalogContextFromContext(ctx)
	if currContext == nil {
		currContext = &CatalogContext{}
	}
	currContext.ViewDefinition = viewDefinition
	return SetCatalogContext(ctx, currContext)
}

// GetCatalogIdFromContext retrieves the catalog ID from the provided context.
func GetCatalogIdFromContext(ctx context.Context) uuid.UUID {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.CatalogId
	}
	return uuid.Nil
}

// GetVariantIdFromContext retrieves the variant ID from the provided context.
func GetVariantIdFromContext(ctx context.Context) uuid.UUID {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.VariantId
	}
	return uuid.Nil
}

// GetNamespaceFromContext retrieves the namespace from the provided context.
func GetNamespaceFromContext(ctx context.Context) string {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.Namespace
	}
	return ""
}

// GetCatalogFromContext retrieves the catalog from the provided context.
func GetCatalogFromContext(ctx context.Context) string {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.Catalog
	}
	return ""
}

// GetVariantFromContext retrieves the variant from the provided context.
func GetVariantFromContext(ctx context.Context) string {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.Variant
	}
	return ""
}

// GetViewDefinitionFromContext retrieves the view definition from the provided context.
func GetViewDefinitionFromContext(ctx context.Context) *types.ViewDefinition {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.ViewDefinition
	}
	return nil
}

// GetUserContextFromContext retrieves the user context from the provided context.
func GetUserContextFromContext(ctx context.Context) *UserContext {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.UserContext
	}
	return nil
}

// SetTestContext sets the test context in the provided context.
func SetTestContext(ctx context.Context, isTest bool) context.Context {
	return context.WithValue(ctx, ctxTestContextKey, isTest)
}

// TestContextFromContext retrieves the test context from the provided context.
func TestContextFromContext(ctx context.Context) bool {
	if testContext, ok := ctx.Value(ctxTestContextKey).(bool); ok {
		return testContext
	}
	return false
}
