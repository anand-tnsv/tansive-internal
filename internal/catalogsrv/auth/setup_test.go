package auth

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
)

func newDb() context.Context {
	ctx := log.Logger.WithContext(context.Background())
	ctx, err := db.ConnCtx(ctx)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("unable to get db connection")
	}
	return ctx
}

func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}

var _ = replaceTabsWithSpaces
