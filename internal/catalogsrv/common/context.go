// Description: This file contains the context package which is used to set and retrieve data from the context.
package common

import (
	"context"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/pkg/types"
)

// ctxTenantIdKeyType represents the key type for the tenant ID in the context.
type ctxTenantIdKeyType string

const ctxTenantIdKey ctxTenantIdKeyType = "HatchCatalogTenantId"

// ctxProjectIdKeyType represents the key type for the project ID in the context.
type ctxProjectIdKeyType string

const ctxProjectIdKey ctxProjectIdKeyType = "HatchCatalogProjectId"

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

type ctxCatalogContextKeyType string

const ctxCatalogContextKey ctxCatalogContextKeyType = "HatchCatalogContext"

type CatalogContext struct {
	CatalogId      uuid.UUID
	VariantId      uuid.UUID
	WorkspaceId    uuid.UUID
	WorkspaceLabel string
	Namespace      string
	Catalog        string
	Variant        string
	ViewDefinition *types.ViewDefinition
	UserContext    *UserContext
}

// UserContext represents the context of an authenticated user in the system.
// It contains information about the user's identity and permissions.
type UserContext struct {
	UserID string
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
	currContext.CatalogId = uuid.UUID(catalogId)
	return SetCatalogContext(ctx, currContext)
}

// SetVariantIdInContext sets the variant ID in the provided context.
func SetVariantIdInContext(ctx context.Context, variantId uuid.UUID) context.Context {
	currContext := CatalogContextFromContext(ctx)
	currContext.VariantId = uuid.UUID(variantId)
	return SetCatalogContext(ctx, currContext)
}

// SetWorkspaceIdInContext sets the workspace ID in the provided context.
func SetWorkspaceIdInContext(ctx context.Context, workspaceId uuid.UUID) context.Context {
	currContext := CatalogContextFromContext(ctx)
	currContext.WorkspaceId = uuid.UUID(workspaceId)
	return SetCatalogContext(ctx, currContext)
}

// SetWorkspaceLabelInContext sets the workspace label in the provided context.
func SetWorkspaceLabelInContext(ctx context.Context, workspaceLabel string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	currContext.WorkspaceLabel = workspaceLabel
	return SetCatalogContext(ctx, currContext)
}

// SetNamespaceInContext sets the namespace in the provided context.
func SetNamespaceInContext(ctx context.Context, namespace string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	currContext.Namespace = namespace
	return SetCatalogContext(ctx, currContext)
}

// SetCatalogInContext sets the catalog in the provided context.
func SetCatalogInContext(ctx context.Context, catalog string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	currContext.Catalog = catalog
	return SetCatalogContext(ctx, currContext)
}

// SetVariantInContext sets the variant in the provided context.
func SetVariantInContext(ctx context.Context, variant string) context.Context {
	currContext := CatalogContextFromContext(ctx)
	currContext.Variant = variant
	return SetCatalogContext(ctx, currContext)
}

// SetViewDefinitionInContext sets the view definition in the provided context.
func SetViewDefinitionInContext(ctx context.Context, viewDefinition *types.ViewDefinition) context.Context {
	currContext := CatalogContextFromContext(ctx)
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

// GetWorkspaceIdFromContext retrieves the workspace ID from the provided context.
func GetWorkspaceIdFromContext(ctx context.Context) uuid.UUID {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.WorkspaceId
	}
	return uuid.Nil
}

// GetWorkspaceLabelFromContext retrieves the workspace label from the provided context.
func GetWorkspaceLabelFromContext(ctx context.Context) string {
	if catalogContext, ok := ctx.Value(ctxCatalogContextKey).(*CatalogContext); ok {
		return catalogContext.WorkspaceLabel
	}
	return ""
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
func GetViewDefinitionFromContext(ctx context.Context) any {
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

type ctxTestContextKeyType string

const ctxTestContextKey ctxTestContextKeyType = "HatchTestContext"

// SetTestContext sets the test context in the provided context.
func SetTestContext(ctx context.Context, b bool) context.Context {

	return context.WithValue(ctx, ctxTestContextKey, b)
}

// TestContextFromContext retrieves the test context from the provided context.
func TestContextFromContext(ctx context.Context) bool {
	if testContext, ok := ctx.Value(ctxTestContextKey).(bool); ok {
		return testContext
	}
	return false
}
