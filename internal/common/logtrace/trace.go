package logtrace

import (
	"context"
)

func RequestIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	r, ok := ctx.Value("requestId").(string)
	if !ok {
		return ""
	}
	return r
}

// TODO - Enable tracing
func IsTraceEnabled() bool {
	return false
}
