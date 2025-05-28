package auth

import (
	"context"

	"github.com/tansive/tansive-internal/pkg/types"
)

type ctxKeyType string

var (
	ViewDefinitionContextKey ctxKeyType = "viewDefinition"
)

func WithViewDefinition(ctx context.Context, viewDefinition *types.ViewDefinition) context.Context {
	return context.WithValue(ctx, ViewDefinitionContextKey, viewDefinition)
}

func GetViewDefinition(ctx context.Context) *types.ViewDefinition {
	return ctx.Value(ViewDefinitionContextKey).(*types.ViewDefinition)
}
