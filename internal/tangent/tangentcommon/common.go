package tangentcommon

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const DefaultConfigFile = "config.toml"

func InitLogger() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
}
