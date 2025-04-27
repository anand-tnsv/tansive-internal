package middleware

import (
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func PanicHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Ctx(r.Context()).Error().Msgf("Panic occurred: %v", err)
				httpx.ErrApplicationError("Unable to process request. Please try again later.").Send(w)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
