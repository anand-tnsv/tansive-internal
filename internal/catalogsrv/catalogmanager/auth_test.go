package catalogmanager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"

	"errors"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
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
	require.NoError(t, err)

	err = db.DB(ctx).CreateProject(ctx, projectID)
	require.NoError(t, err)

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

	// Create a catalog for testing
	var info pgtype.JSONB
	err = info.Set(map[string]interface{}{"meta": "test"})
	require.NoError(t, err)

	catalogID := uuid.New()
	catalog := &models.Catalog{
		CatalogID:   catalogID,
		Name:        "test-catalog",
		Description: "Test catalog",
		ProjectID:   projectID,
		Info:        info,
	}
	err = db.DB(ctx).CreateCatalog(ctx, catalog)
	require.NoError(t, err)

	// Create parent view
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

	// Convert parent view to JSON
	parentViewJSON, err := json.Marshal(parentView)
	require.NoError(t, err)

	// Create the view model
	view := &models.View{
		Label:       "parent-view",
		Description: "Parent view for testing",
		Rules:       parentViewJSON,
		CatalogID:   catalogID,
		TenantID:    tenantID,
	}

	// Store the parent view in the database
	err = db.DB(ctx).CreateView(ctx, view)
	require.NoError(t, err)

	log.Ctx(ctx).Info().
		Str("view_id", view.ViewID.String()).
		Str("catalog_id", catalogID.String()).
		Msg("Created view successfully")

	// Create an active signing key
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	encKey, err := common.Encrypt(priv, cfg.KeyEncryptionPasswd)
	require.NoError(t, err)

	signingKey := &models.SigningKey{
		PublicKey:  pub,
		PrivateKey: encKey,
		IsActive:   true,
	}
	err = db.DB(ctx).CreateSigningKey(ctx, signingKey)
	require.NoError(t, err)

	return ctx, tenantID, projectID, view.ViewID, cfg
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
		require.NoError(t, appErr)
		require.NotEmpty(t, token)

		// Get the signing key from database
		dbKey, dbErr := db.DB(ctx).GetActiveSigningKey(ctx)
		require.NoError(t, dbErr)
		require.NotNil(t, dbKey)

		// Parse and verify the token
		parsedToken, parseErr := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodEd25519); !ok {
				return nil, errors.New("unexpected signing method")
			}
			return ed25519.PublicKey(dbKey.PublicKey), nil
		})
		require.NoError(t, parseErr)
		require.True(t, parsedToken.Valid)

		// Extract claims
		claims, ok := parsedToken.Claims.(jwt.MapClaims)
		require.True(t, ok)

		// Verify token ID and view ID
		jti, ok := claims["jti"].(string)
		require.True(t, ok)
		require.NotEmpty(t, jti)

		sub, ok := claims["sub"].(string)
		require.True(t, ok)
		require.Equal(t, testViewID.String(), sub)

		// Verify token is stored in database
		storedToken, dbErr := db.DB(ctx).GetViewToken(ctx, uuid.MustParse(jti))
		require.NoError(t, dbErr)
		require.Equal(t, testViewID, storedToken.ViewID)
	})

	t.Run("invalid view", func(t *testing.T) {
		token, err := CreateToken(ctx, nil, testViewID)
		assert.Error(t, err)
		assert.Empty(t, token)
	})
}
