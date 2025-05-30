package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/rs/zerolog/log"
)

type requestIdContextKey string

const (
	requestIdKey    = requestIdContextKey("requestId")
	RequestIDHeader = "X-Tansive-Request-ID"
)

// RequestLogger logs the request and adds a request ID to context and response header.
func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()

		requestID := newRequestId()
		ctx = context.WithValue(ctx, requestIdKey, requestID)
		ctx = log.With().Str("request_id", requestID).Caller().Logger().WithContext(ctx)

		w.Header().Set(RequestIDHeader, requestID)

		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		requestURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.RequestURI)
		requestFields := map[string]any{
			"requestURL":    requestURL,
			"requestMethod": r.Method,
			"requestPath":   r.URL.Path,
			"remoteIP":      r.RemoteAddr,
			"proto":         r.Proto,
		}
		log.Ctx(ctx).Info().Fields(requestFields).Msg("incoming request")

		defer func() {
			log.Ctx(ctx).Info().
				Str("duration", fmt.Sprintf("%dms", time.Since(start).Milliseconds())).
				Msg("request completed")
		}()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newRequestId() string {
	u, err := uuid.NewRandom()
	if err == nil {
		return u.String()
	}
	return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
}
