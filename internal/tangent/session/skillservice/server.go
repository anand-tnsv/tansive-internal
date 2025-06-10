package skillservice

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/tansive/tansive-internal/internal/tangent/session/proto"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultSocketName = "tangent.service"
)

// GetSocketPath returns the appropriate socket path using XDG_RUNTIME_DIR if available
func GetSocketPath() (string, error) {
	// Try XDG_RUNTIME_DIR first
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

// Server represents a generic gRPC server that can handle multiple services
type Server struct {
	grpcServer *grpc.Server
	socketPath string
	services   []interface{}
}

// NewServer creates a new generic gRPC server
func NewServer() (*Server, error) {
	socketPath, err := GetSocketPath()
	if err != nil {
		return nil, fmt.Errorf("failed to determine socket path: %w", err)
	}
	return &Server{
		socketPath: socketPath,
		services:   make([]interface{}, 0),
	}, nil
}

// RegisterService registers a service with the gRPC server
func (s *Server) RegisterService(service interface{}) {
	s.services = append(s.services, service)
}

// Start starts the gRPC server on a Unix domain socket
func (s *Server) Start() error {
	socketDir := filepath.Dir(s.socketPath)
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	if err := os.RemoveAll(s.socketPath); err != nil {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	lis, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %w", err)
	}

	if err := os.Chmod(s.socketPath, 0600); err != nil {
		log.Warn().Err(err).Msg("failed to chmod socket file")
	}

	s.grpcServer = grpc.NewServer()

	// Register all services
	for _, service := range s.services {
		switch svc := service.(type) {
		case proto.SkillServiceServer:
			proto.RegisterSkillServiceServer(s.grpcServer, svc)
		// Add more service types here as needed
		default:
			log.Warn().Type("service", service).Msg("unknown service type, skipping registration")
		}
	}

	reflection.Register(s.grpcServer)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("received shutdown signal")
		s.Stop()
	}()

	log.Info().Str("socket", s.socketPath).Msg("gRPC server started")

	if err := s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

// Stop gracefully stops the gRPC server and cleans up the socket file
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if err := os.Remove(s.socketPath); err != nil {
		log.Warn().Err(err).Str("socket", s.socketPath).Msg("failed to remove socket file")
	}
}

// SkillService implements the SkillService RPC methods
type SkillService struct {
	proto.UnimplementedSkillServiceServer
	skillManager tangentcommon.SkillManager
}

// NewSkillService creates a new skill service instance
func NewSkillService(skillManager tangentcommon.SkillManager) *SkillService {
	if skillManager == nil {
		log.Error().Msg("SkillManager is nil")
		return nil
	}
	return &SkillService{
		skillManager: skillManager,
	}
}

// InvokeSkill implements the SkillService RPC method
func (s *SkillService) InvokeSkill(ctx context.Context, req *proto.SkillInvocation) (*proto.SkillResult, error) {
	skillManager := s.skillManager
	if skillManager == nil {
		log.Error().Msg("SkillManager is nil")
		return nil, fmt.Errorf("SkillManager is nil")
	}

	response, err := skillManager.Run(ctx, &tangentcommon.RunParams{
		SessionID:    req.SessionId,
		InvocationID: req.InvocationId,
		SkillName:    req.SkillName,
		InputArgs:    req.Args.AsMap(),
	})

	if err != nil {
		// we don't return as the error has to be returned to the client
		log.Error().Err(err).Msg("failed to run skill")
	}

	// Convert response to protobuf struct
	outputStruct, err2 := structpb.NewStruct(response)
	if err2 != nil {
		log.Error().Err(err2).Msg("failed to create output struct")
		return nil, err2
	}

	return &proto.SkillResult{
		InvocationId: req.InvocationId,
		Output:       outputStruct,
	}, err
}
