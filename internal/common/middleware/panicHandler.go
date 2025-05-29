package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func PanicHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := httpx.NewResponseWriter(w)
		defer func() {
			if err := recover(); err != nil {
				stack := debug.Stack()

				log.Ctx(r.Context()).Error().
					Str("panic", fmt.Sprintf("%v", err)).
					Str("stack_trace", string(stack)).
					Msg("panic occurred")

				if !rw.Written() {
					httpx.ErrApplicationError("unable to process request").Send(rw)
				}
			}
		}()
		next.ServeHTTP(rw, r)
	})
}
