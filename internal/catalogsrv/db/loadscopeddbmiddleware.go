package db

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

// LoadScopedDBMiddleware is a middleware that loads a scoped db connection from the request context
// and closes it after the request is served.
func LoadScopedDBMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := ConnCtx(r.Context())
		if err != nil {
			log.Ctx(r.Context()).Fatal().Err(err).Msg("unable to get db connection")
			httpx.ErrApplicationError("unable to service request at this time").Send(w)
			return
		}
		defer func() {
			if dbConn := DB(ctx); dbConn != nil {
				log.Ctx(r.Context()).Info().Msg("closing db connection")
				fmt.Println("closing db connection")
				dbConn.Close(context.Background()) // use background to avoid canceled context
			} else {
				log.Ctx(r.Context()).Info().Msg("db connection already closed")
				fmt.Println("db connection already closed")
			}
		}()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
