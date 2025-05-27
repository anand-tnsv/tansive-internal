package dbmanager

import (
	"context"
	"database/sql"

	"github.com/rs/zerolog/log"
)

type ScopedDb interface {
	// Conn returns a new connection to the database.
	// Returns a ScopedConn and an error, if any.
	Conn(ctx context.Context) (ScopedConn, error)
	// Stats returns the number of connection requests and returns.
	Stats() (requests, returns uint64)
}

type ScopedConn interface {
	// AddScopes adds the given scopes to the connection.
	AddScopes(ctx context.Context, scopes map[string]string) error
	// DropScopes drops the given scopes from the connection.
	DropScopes(ctx context.Context, scopes []string) error
	// AddScope adds the given scope with the given value to the connection.
	AddScope(ctx context.Context, scope, value string) error
	// DropScope drops the given scope from the connection.
	DropScope(ctx context.Context, scope string) error
	// DropAllScopes drops all scopes from the connection.
	DropAllScopes(ctx context.Context) error
	// Conn returns the underlying connection of the ScopedConn.
	Conn() *sql.Conn
	// Close drops all scopes and returns the connection back to the pool.
	Close(ctx context.Context)
}

func NewScopedDb(ctx context.Context, dbtype string, configuredScopes []string) ScopedDb {
	switch dbtype {
	case "postgresql":
		db, err := NewPostgresqlDb(configuredScopes)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("Failed to create PostgreSQL DB")
			return nil
		}
		return db
	}
	return nil
}
