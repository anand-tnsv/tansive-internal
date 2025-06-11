package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog/log"

	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/common/logtrace"
	"github.com/tansive/tansive-internal/internal/common/middleware"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/session"
)

type AgentServer struct {
	Router *chi.Mux
}

func CreateNewServer() (*AgentServer, error) {
	s := &AgentServer{}
	s.Router = chi.NewRouter()
	return s, nil
}

func (s *AgentServer) MountHandlers() {
	s.Router.Use(middleware.RequestLogger)
	s.Router.Use(middleware.PanicHandler)
	if config.Config().HandleCORS {
		s.Router.Use(s.HandleCORS)
	}
	s.mountResourceHandlers(s.Router)
	if logtrace.IsTraceEnabled() {
		fmt.Println("Routes in tenant router")
		walkFunc := func(method string, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
			fmt.Printf("%s %s\n", method, route)
			return nil
		}
		if err := chi.Walk(s.Router, walkFunc); err != nil {
			log.Error().Err(err).Msg("Error walking router")
		}
	}
}

func (s *AgentServer) mountResourceHandlers(r chi.Router) {
	r.Route("/sessions", func(r chi.Router) {
		session.Router(r)
	})
	r.Get("/version", s.getVersion)
	r.Get("/ready", s.getReadiness)
}

type GetVersionRsp struct {
	ServerVersion string `json:"serverVersion"`
	ApiVersion    string `json:"apiVersion"`
}

func (s *AgentServer) getVersion(w http.ResponseWriter, r *http.Request) {
	log.Ctx(r.Context()).Debug().Msg("GetVersion")
	rsp := &GetVersionRsp{
		ServerVersion: "Tansive Tangent Server: 0.1.0",
		ApiVersion:    "v1alpha1",
	}
	httpx.SendJsonRsp(r.Context(), w, http.StatusOK, rsp)
}

func (s *AgentServer) getReadiness(w http.ResponseWriter, r *http.Request) {
	log.Ctx(r.Context()).Debug().Msg("Readiness check")

	// Add any specific readiness checks here
	// For now, we'll just return ready if the server is up
	httpx.SendJsonRsp(r.Context(), w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}

func (s *AgentServer) HandleCORS(next http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"}, //TODO: Change this to specific origin
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "Content-Length", "Accept-Encoding"},
		ExposedHeaders:   []string{"Link", "Location", "X-Tansive-Request-Id"},
		AllowCredentials: false,
		MaxAge:           300,
	})(next)
}
