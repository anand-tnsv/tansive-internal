package catalogmanager

import (
	"context"
	"crypto/ed25519"
	"testing"

	"errors"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/pkg/types"
)

func setupTest(t *testing.T) (context.Context, types.TenantId, types.ProjectId, uuid.UUID, *config.ConfigParam) {
	// Initialize context with logger and database connection
	ctx := newDb()

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)

	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)

	// Register cleanup function that will run even if test panics
	t.Cleanup(func() {
		db.DB(ctx).DeleteProject(ctx, projectID)
		db.DB(ctx).DeleteTenant(ctx, tenantID)
		db.DB(ctx).Close(ctx)
	})

	// Set up test configuration
	cfg := config.Config()
	cfg.DefaultTokenValidity = "1h"
	cfg.ServerHostName = "localhost"
	cfg.ServerPort = "8080"
	cfg.KeyEncryptionPasswd = "test-password"

	// Create test data
	testViewID := uuid.New()
	parentView := &ViewDefinition{
		Scope: ViewScope{
			Catalog: "test-catalog",
		},
		Rules: ViewRuleSet{
			{
				Intent:  IntentAllow,
				Actions: []Action{ActionCatalogList, ActionVariantList},
				Targets: []TargetResource{"res://catalogs/test-catalog"},
			},
		},
	}

	// Add parent view to context
	ctx = addViewToContext(ctx, parentView)

	return ctx, tenantID, projectID, testViewID, cfg
}

func TestCreateToken(t *testing.T) {
	ctx, _, _, testViewID, _ := setupTest(t)

	t.Run("successful token creation", func(t *testing.T) {
		derivedView := &ViewDefinition{
			Scope: ViewScope{
				Catalog: "test-catalog",
			},
			Rules: ViewRuleSet{
				{
					Intent:  IntentAllow,
					Actions: []Action{ActionCatalogList},
					Targets: []TargetResource{"res://catalogs/test-catalog"},
				},
			},
		}

		token, appErr := CreateToken(ctx, derivedView, testViewID)
		assert.NoError(t, appErr)
		assert.NotEmpty(t, token)

		// Get the signing key from database
		dbKey, dbErr := db.DB(ctx).GetActiveSigningKey(ctx)
		assert.NoError(t, dbErr)
		assert.NotNil(t, dbKey)

		// Parse and verify the token
		parsedToken, parseErr := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodEd25519); !ok {
				return nil, errors.New("unexpected signing method")
			}
			return ed25519.PublicKey(dbKey.PublicKey), nil
		})
		assert.NoError(t, parseErr)
		assert.True(t, parsedToken.Valid)

		// Extract claims
		claims, ok := parsedToken.Claims.(jwt.MapClaims)
		assert.True(t, ok)

		// Verify token ID and view ID
		jti, ok := claims["jti"].(string)
		assert.True(t, ok)
		assert.NotEmpty(t, jti)

		sub, ok := claims["sub"].(string)
		assert.True(t, ok)
		assert.Equal(t, testViewID.String(), sub)

		// Verify token is stored in database
		storedToken, dbErr := db.DB(ctx).GetViewToken(ctx, uuid.MustParse(jti))
		assert.NoError(t, dbErr)
		assert.Equal(t, testViewID, storedToken.ViewID)
	})

	t.Run("invalid view", func(t *testing.T) {
		token, err := CreateToken(ctx, nil, testViewID)
		assert.Error(t, err)
		assert.Empty(t, token)
	})
}
