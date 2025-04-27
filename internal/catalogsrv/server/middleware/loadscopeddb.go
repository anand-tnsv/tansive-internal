package middleware

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
)

func LoadScopedDB(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := db.ConnCtx(r.Context())
		defer db.DB(ctx).Close(ctx)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
