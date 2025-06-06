package session

import (
	"context"
)

type SessionLogger interface {
	Info(ctx context.Context, msg string, args ...interface{})
	Error(ctx context.Context, msg string, args ...interface{})
}
