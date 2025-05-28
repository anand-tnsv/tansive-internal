package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/server"
	"github.com/tansive/tansive-internal/internal/common/logtrace"
)

func init() {
	logtrace.InitLogger()
}

type cmdoptions struct {
	configFile *string
}

func main() {

	slog := log.With().Str("state", "init").Logger()
	// Parse command line flags
	opt := parseFlags()

	slog.Info().Str("config_file", *opt.configFile).Msg("loading config file")
	// load config file
	if err := config.LoadConfig(*opt.configFile); err != nil {
		slog.Error().Str("config_file", *opt.configFile).Err(err).Msg("unable to load config file")
		os.Exit(1)
	}
	if config.Config().ServerPort == "" {
		slog.Error().Msg("server port not defined")
		os.Exit(1)
	}
	if config.Config().SingleUserMode {
		slog.Info().Msg("single user mode enabled")
		err := createDefaultTenantAndProject()
		if err != nil {
			slog.Error().Err(err).Msg("unable to create default tenant and project")
			os.Exit(1)
		}
	}
	s, err := server.CreateNewServer()
	if err != nil {
		slog.Error().Err(err).Msg("Unable to create server")
	}
	s.MountHandlers()
	http.ListenAndServe(":"+config.Config().ServerPort, s.Router)
}

func createDefaultTenantAndProject() error {
	ctx, err := db.ConnCtx(context.Background())
	if err != nil {
		return err
	}
	defer db.DB(ctx).Close(ctx)
	if err := db.DB(ctx).CreateTenant(ctx, catcommon.TenantId(config.Config().DefaultTenantID)); err != nil {
		if err != dberror.ErrAlreadyExists {
			return err
		}
	}
	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(config.Config().DefaultTenantID))
	if err := db.DB(ctx).CreateProject(ctx, catcommon.ProjectId(config.Config().DefaultProjectID)); err != nil {
		if err != dberror.ErrAlreadyExists {
			return err
		}
	}
	return nil
}

const DefaultConfigFile = "/etc/tansive/tansivesrv.conf"

func parseFlags() cmdoptions {
	var opt cmdoptions
	opt.configFile = flag.String("config", DefaultConfigFile, "Path to the config file")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options]\n\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
	}
	flag.Parse()
	return opt
}
