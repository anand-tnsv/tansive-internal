package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tansive/tansive-internal/internal/tangent/common"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/server"

	"github.com/rs/zerolog/log"
)

func init() {
	common.InitLogger()
}

type cmdoptions struct {
	configFile string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx); err != nil {
		log.Error().Err(err).Msg("server failed")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	slog := log.With().Str("state", "init").Logger()

	opt := parseFlags()

	slog.Info().Str("config_file", opt.configFile).Msg("loading config file")
	if err := config.LoadConfig(opt.configFile); err != nil {
		return fmt.Errorf("loading config file: %w", err)
	}
	if config.Config().ServerPort == "" {
		return fmt.Errorf("server port not defined")
	}

	s, err := server.CreateNewServer()
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}
	s.MountHandlers()

	srv := &http.Server{
		Addr:    ":" + config.Config().ServerPort,
		Handler: s.Router,
	}

	// Channel to listen for errors coming from the listener.
	serverErrors := make(chan error, 1)

	// Start the service listening for requests.
	go func() {
		slog.Info().Str("port", config.Config().ServerPort).Msg("server started")
		serverErrors <- srv.ListenAndServe()
	}()

	// Channel to listen for an interrupt or terminate signal from the OS.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Wait forever until shutdown
	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)

	case sig := <-shutdown:
		slog.Info().Str("signal", sig.String()).Msg("shutdown signal received")

		// Give outstanding requests 5 seconds to complete.
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error().Err(err).Msg("could not stop server gracefully")
			if err := srv.Close(); err != nil {
				slog.Error().Err(err).Msg("could not stop server")
			}
		}
	}

	slog.Info().Msg("server stopped")
	return nil
}

const DefaultConfigFile = "/etc/tansive/tangent.conf"

func parseFlags() cmdoptions {
	var opt cmdoptions
	flag.StringVar(&opt.configFile, "config", DefaultConfigFile, "Path to the config file")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options]\n\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
	}
	flag.Parse()
	return opt
}
