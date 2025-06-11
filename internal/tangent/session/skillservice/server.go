package skillservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
)

const defaultSocketName = "tangent.service"

type SkillService struct {
	skillManager tangentcommon.SkillManager
	Router       *chi.Mux
	server       *http.Server
	socketPath   string
	mu           sync.Mutex
}

func NewSkillService(skillManager tangentcommon.SkillManager) *SkillService {
	if skillManager == nil {
		log.Error().Msg("SkillManager is nil")
		return nil
	}
	return &SkillService{
		skillManager: skillManager,
		Router:       chi.NewRouter(),
	}
}

type skillInvocation struct {
	SessionID    string         `json:"session_id"`
	InvocationID string         `json:"invocation_id"`
	SkillName    string         `json:"skill_name"`
	Args         map[string]any `json:"args"`
}

type skillResult struct {
	InvocationID string         `json:"invocation_id"`
	Output       map[string]any `json:"output"`
}

func (s *SkillService) handleInvokeSkill(r *http.Request) (*httpx.Response, error) {
	var req skillInvocation
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, ErrInvalidRequest.Msg(err.Error())
	}

	resp, err := s.skillManager.Run(r.Context(), &tangentcommon.RunParams{
		SessionID:    req.SessionID,
		InvocationID: req.InvocationID,
		SkillName:    req.SkillName,
		InputArgs:    req.Args,
	})

	if err != nil {
		return nil, ErrSkillServiceError.Msg(err.Error())
	}

	result := skillResult{
		InvocationID: req.InvocationID,
		Output:       resp,
	}

	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   result,
	}, nil
}

func (s *SkillService) handleGetTools(r *http.Request) (*httpx.Response, error) {
	tools, err := s.skillManager.GetTools(r.Context(), r.URL.Query().Get("session_id"))
	if err != nil {
		return nil, ErrSkillServiceError.Msg(err.Error())
	}
	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   tools,
	}, nil
}

func (s *SkillService) handleGetContext(r *http.Request) (*httpx.Response, error) {
	sessionID := r.URL.Query().Get("session_id")
	name := r.URL.Query().Get("name")
	context, err := s.skillManager.GetContext(r.Context(), sessionID, name)
	if err != nil {
		return nil, ErrSkillServiceError.Msg(err.Error())
	}
	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   context,
	}, nil
}

func GetSocketPath() (string, error) {
	if xdgRuntimeDir := os.Getenv("XDG_RUNTIME_DIR"); xdgRuntimeDir != "" {
		if _, err := os.Stat(xdgRuntimeDir); err == nil {
			return filepath.Join(xdgRuntimeDir, defaultSocketName), nil
		}
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}
	runtimeDir := filepath.Join(homeDir, ".local", "run")
	if err := os.MkdirAll(runtimeDir, 0700); err != nil {
		return "", fmt.Errorf("failed to create runtime directory: %w", err)
	}
	return filepath.Join(runtimeDir, defaultSocketName), nil
}

func (s *SkillService) MountHandlers() {
	s.Router.Post("/skill-invocations", httpx.WrapHttpRsp(s.handleInvokeSkill))
	s.Router.Get("/tools", httpx.WrapHttpRsp(s.handleGetTools))
	s.Router.Get("/context", httpx.WrapHttpRsp(s.handleGetContext))
}

func (s *SkillService) StartServer() error {
	socketPath, err := GetSocketPath()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.socketPath = socketPath
	s.mu.Unlock()

	// Remove existing socket if it exists
	if _, err := os.Stat(socketPath); err == nil {
		if err := os.Remove(socketPath); err != nil {
			return fmt.Errorf("failed to remove existing socket: %w", err)
		}
	}

	socketDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on unix socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		log.Warn().Err(err).Msg("failed to chmod socket")
	}

	s.MountHandlers()
	srv := &http.Server{Handler: s.Router}
	s.mu.Lock()
	s.server = srv
	s.mu.Unlock()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("received shutdown signal")
		s.StopServer()
	}()

	log.Info().Str("socket", socketPath).Msg("REST server started")

	// Start server and handle shutdown
	err = srv.Serve(listener)
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

func (s *SkillService) StopServer() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("error shutting down server")
		}
		s.server = nil
	}

	if s.socketPath != "" {
		if _, err := os.Stat(s.socketPath); err == nil {
			if err := os.Remove(s.socketPath); err != nil {
				log.Error().Err(err).Msg("error removing socket file")
			}
		}
		s.socketPath = ""
	}
}
