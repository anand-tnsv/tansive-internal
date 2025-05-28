package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/apis"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/server/middleware"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/common/logtrace"
	commonmiddleware "github.com/tansive/tansive-internal/internal/common/middleware"
)

type CatalogServer struct {
	Router *chi.Mux
}

func CreateNewServer() (*CatalogServer, error) {
	s := &CatalogServer{}
	s.Router = chi.NewRouter()
	return s, nil
}

func (s *CatalogServer) MountHandlers() {
	s.Router.Use(commonmiddleware.RequestLogger)
	s.Router.Use(commonmiddleware.PanicHandler)
	// s.Router.Use(commonmiddleware.SetTimeout(5 * time.Second))
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

func (s *CatalogServer) mountResourceHandlers(r chi.Router) {
	r.Use(middleware.LoadScopedDB) // Load the scoped db connection
	r.Mount("/auth", auth.Router(r))
	r.Mount("/", apis.Router(r))
	r.Get("/version", s.getVersion)
}

type GetVersionRsp struct {
	ServerVersion string `json:"serverVersion"`
	ApiVersion    string `json:"apiVersion"`
}

func (s *CatalogServer) getVersion(w http.ResponseWriter, r *http.Request) {
	log.Ctx(r.Context()).Debug().Msg("GetVersion")
	rsp := &GetVersionRsp{
		ServerVersion: "Tansive Catalog Server: 0.1.0", //TODO - Implement server versioning
		ApiVersion:    "v1alpha1",
	}
	httpx.SendJsonRsp(r.Context(), w, http.StatusOK, rsp)
}

func (s *CatalogServer) HandleCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8190")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")                                                       // Allowed methods
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-Hatch-IDToken") // Allowed headers

		// Check if the request method is OPTIONS
		if r.Method == "OPTIONS" {
			log.Ctx(r.Context()).Debug().Msg("OPTIONS request")
			// Respond with appropriate headers and no body
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
