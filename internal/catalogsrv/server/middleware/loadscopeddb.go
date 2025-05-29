package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func LoadScopedDB(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := db.ConnCtx(r.Context())
		if err != nil {
			log.Ctx(r.Context()).Fatal().Err(err).Msg("unable to get db connection")
			httpx.ErrApplicationError("unable to service request at this time").Send(w)
			return
		}
		defer func() {
			if dbConn := db.DB(ctx); dbConn != nil {
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
