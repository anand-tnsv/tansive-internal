package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"

	"github.com/tansive/tansive-internal/internal/common/logtrace"
	hatchmiddleware "github.com/tansive/tansive-internal/internal/common/middleware"
	"github.com/tansive/tansive-internal/internal/worker/config"
	"github.com/tansive/tansive-internal/internal/worker/session"
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
	s.Router.Use(hatchmiddleware.RequestLogger)
	s.Router.Use(hatchmiddleware.PanicHandler)
	if config.Config().HandleCORS {
		s.Router.Use(s.HandleCORS)
	}
	s.Router.Route("/", s.mountResourceHandlers)
	if logtrace.IsTraceEnabled() {
		//print all the routes in the router by transversing the tree and printing the patterns
		fmt.Println("Routes in tenant router")
		walkFunc := func(method string, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
			fmt.Printf("%s %s\n", method, route)
			return nil
		}
		if err := chi.Walk(s.Router, walkFunc); err != nil {
			fmt.Printf("Logging err: %s\n", err.Error())
		}
	}
}

func (s *AgentServer) mountResourceHandlers(r chi.Router) {
	r.Route("/sessions", func(r chi.Router) {
		session.Router(r)
	})
}

func (s *AgentServer) HandleCORS(next http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"}, //TODO: Change this to specific origin
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "Content-Length", "Accept-Encoding"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	})(next)
}
