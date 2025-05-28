package auth

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
)

type ctxKeyType string

var (
	ViewDefinitionContextKey ctxKeyType = "viewDefinition"
)

func WithViewDefinition(ctx context.Context, viewDefinition *policy.ViewDefinition) context.Context {
	return context.WithValue(ctx, ViewDefinitionContextKey, viewDefinition)
}

func GetViewDefinition(ctx context.Context) *policy.ViewDefinition {
	return ctx.Value(ViewDefinitionContextKey).(*policy.ViewDefinition)
}
