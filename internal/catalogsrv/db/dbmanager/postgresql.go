// Package dbmanager provides functionality for managing the PostgreSQL database connection pool and executing queries.
package dbmanager

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/tansive/tansive-internal/internal/catalogsrv/db/config"
	"github.com/rs/zerolog/log"
)

// PostgresConn represents a connection to the PostgreSQL database.
type postgresConn struct {
	conn             *sql.Conn
	cancel           context.CancelFunc
	scopes           map[string]string
	configuredScopes []string
	pool             *postgresPool
}

// PostgresPool represents a pool of PostgreSQL database connections.
type postgresPool struct {
	configuredScopes []string
	connRequests     uint64
	connReturns      uint64
	db               *sql.DB
}

// NewPostgresqlDb creates a new PostgreSQL database connection pool with the given configured scopes.
// It returns a pointer to the PostgresPool and an error, if any.
func NewPostgresqlDb(configuredScopes []string) (ScopedDb, error) {
	// Get the database connection string from the configuration.
	dsn := config.HatchCatalogDsn()

	// Open a new database connection using the "pgx" driver.
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error().Err(err).Msg("failed to open db")
		return nil, err
	}

	// Ping the database to see if the connection is valid.
	err = sqlDB.Ping()
	if err != nil {
		log.Error().Err(err).Msg("failed to ping db")
		return nil, err
	}

	// Return the PostgresPool with the configured scopes and the database connection.
	return &postgresPool{
		configuredScopes: configuredScopes,
		db:               sqlDB,
	}, nil
}

// Conn returns a new connection to the PostgreSQL database from the connection pool.
func (p *postgresPool) Conn(ctx context.Context) (ScopedConn, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Obtain a connection from the database connection pool.
	conn, err := p.db.Conn(ctx)
	defer func() {
		if r := recover(); r != nil {
			cancel()
			conn.Close()
		}
	}()

	if err != nil {
		log.Error().Err(err).Msg("failed to obtain connection")
		cancel()
		return nil, err
	}

	// set lock timeout for conn
	_, err = conn.ExecContext(ctx, "SET lock_timeout = '5s'")
	if err != nil {
		log.Error().Err(err).Msg("failed to set lock timeout")
		cancel()
		return nil, err
	}
	// set statement timeout for conn
	_, err = conn.ExecContext(ctx, "SET statement_timeout = '5s'")
	if err != nil {
		log.Error().Err(err).Msg("failed to set statement timeout")
		cancel()
		return nil, err
	}
	// TODO: set idle_in_transaction_session_timeout for conn
	// Create a new PostgresConn instance with the configured scopes and the obtained connection.
	h := &postgresConn{
		configuredScopes: p.configuredScopes,
		scopes:           make(map[string]string),
		cancel:           cancel,
		pool:             p,
		conn:             conn,
	}

	// Clean up the scopes, just in case.
	err = h.DropScopes(ctx, p.configuredScopes)
	if err != nil {
		panic(err)
	}

	p.connRequests++
	return h, nil
}

// Stats returns the number of connection requests and returns made to the PostgreSQL database.
func (p *postgresPool) Stats() (requests, returns uint64) {
	return p.connRequests, p.connReturns
}

// Close cleans up the scopes and returns the connection back to the pool.
func (h *postgresConn) Close(ctx context.Context) {
	h.DropAllScopes(ctx)
	if h.cancel != nil {
		h.cancel()
	}
	if h.conn != nil {
		h.conn.Close()
	}
	h.pool.connReturns++
}

// IsConfiguredScope checks if the given scope is configured in the PostgresConn.
func (h *postgresConn) IsConfiguredScope(scope string) bool {
	for _, s := range h.configuredScopes {
		if s == scope {
			return true
		}
	}
	return false
}

// AddScopes adds the given scopes to the PostgresConn.
func (h *postgresConn) AddScopes(ctx context.Context, scopes map[string]string) {
	if h.conn == nil {
		return
	}
	for scope, value := range scopes {
		if h.IsConfiguredScope(scope) {
			sqlCmd := fmt.Sprintf("SET %s TO $1", scope)
			_, err := h.conn.ExecContext(ctx, sqlCmd, value)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to set scope")
				panic(err)
			}
			h.scopes[scope] = value
		}
	}
}

// AddScope adds a single scope to the PostgresConn.
func (h *postgresConn) AddScope(ctx context.Context, scope, value string) {
	if h.conn == nil {
		return
	}
	if h.IsConfiguredScope(scope) {
		sqlCmd := fmt.Sprintf("SET %s TO $1", scope)
		_, err := h.conn.ExecContext(ctx, sqlCmd, value)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to set scope")
			panic(err)
		}
		h.scopes[scope] = value
	}
}

// AuthorizedScopes returns the currently authorized scopes in the PostgresConn.
func (h *postgresConn) AuthorizedScopes() map[string]string {
	return h.scopes
}

// DropScopes drops the given scopes from the PostgresConn.
func (h *postgresConn) DropScopes(ctx context.Context, scopes []string) error {
	if h.conn == nil {
		log.Ctx(ctx).Error().Msg("no connection")
		return nil // don't return error and panic
	}
	for _, scope := range scopes {
		sqlCmd := fmt.Sprintf("RESET %s", scope)
		_, err := h.conn.ExecContext(ctx, sqlCmd)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to reset scope")
			return err
		}
		delete(h.scopes, scope)
	}
	return nil
}

// DropScope drops a single scope from the PostgresConn.
func (h *postgresConn) DropScope(ctx context.Context, scope string) error {
	if h.conn == nil {
		return nil // don't return error and panic
	}
	sqlCmd := fmt.Sprintf("RESET %s", scope)
	_, err := h.conn.ExecContext(ctx, sqlCmd)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to reset scope")
		return err
	}
	delete(h.scopes, scope)
	return nil
}

// DropAllScopes drops all the configured scopes from the PostgresConn.
func (h *postgresConn) DropAllScopes(ctx context.Context) error {
	return h.DropScopes(ctx, h.configuredScopes)
}

// Conn returns the underlying connection of the PostgresConn.
func (h *postgresConn) Conn() *sql.Conn {
	return h.conn
}
