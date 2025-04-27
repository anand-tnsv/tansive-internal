package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type requestIdContextKey string

const requestIdKey = requestIdContextKey("requestId")

// RequestLogger is a middleware that logs the request details and adds a unique request ID to the context.
func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Generate a unique uuid. don't use common.uuid() as it is not defined in this package
		requestID := newRequestId()
		// Add the request ID to the request context
		ctx = context.WithValue(ctx, requestIdKey, requestID)
		// Add a sub-logger with requestId to context
		ctx = log.With().Str("request_id", requestID).Caller().Logger().WithContext(ctx)
		// Include the request ID in the response header
		w.Header().Set("X-Tansive-Request-ID", requestID)

		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		requestURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.RequestURI)
		requestFields := map[string]interface{}{
			"requestURL":    requestURL,
			"requestMethod": r.Method,
			"requestPath":   r.URL.Path,
			"remoteIP":      r.RemoteAddr,
			"proto":         r.Proto,
		}
		log.Ctx(ctx).Info().Fields(requestFields).Msg("")
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newRequestId() string {
	u, err := uuid.NewRandom()
	if err == nil {
		return u.String()
	} else {
		return ""
	}
}
