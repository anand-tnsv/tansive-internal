package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func PanicHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				stack := debug.Stack()

				log.Ctx(r.Context()).Error().
					Str("stack", string(stack)).
					Msgf("panic occurred: %v", err)

				httpx.ErrApplicationError("unable to process request. please try again later.").Send(w)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
