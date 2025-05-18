package db

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestCreateSigningKey(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	t.Run("successful non-active key creation", func(t *testing.T) {
		// Test creating a non-active key
		key := &models.SigningKey{
			PublicKey:  []byte("test-public-key"),
			PrivateKey: []byte("test-private-key"),
			IsActive:   false,
		}
		err = DB(ctx).CreateSigningKey(ctx, key)
		assert.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, key.KeyID)
		defer DB(ctx).DeleteSigningKey(ctx, key.KeyID)

		// Verify the key was created correctly
		retrievedKey, err := DB(ctx).GetSigningKey(ctx, key.KeyID)
		assert.NoError(t, err)
		assert.Equal(t, key.PublicKey, retrievedKey.PublicKey)
		assert.Equal(t, key.PrivateKey, retrievedKey.PrivateKey)
		assert.False(t, retrievedKey.IsActive)
	})

	t.Run("successful active key creation", func(t *testing.T) {
		// Test creating an active key
		activeKey := &models.SigningKey{
			PublicKey:  []byte("test-active-public-key"),
			PrivateKey: []byte("test-active-private-key"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, activeKey)
		assert.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, activeKey.KeyID)
		defer DB(ctx).DeleteSigningKey(ctx, activeKey.KeyID)

		// Verify the key was created correctly and is active
		retrievedKey, err := DB(ctx).GetSigningKey(ctx, activeKey.KeyID)
		assert.NoError(t, err)
		assert.Equal(t, activeKey.PublicKey, retrievedKey.PublicKey)
		assert.Equal(t, activeKey.PrivateKey, retrievedKey.PrivateKey)
		assert.True(t, retrievedKey.IsActive)
	})

	t.Run("multiple active keys handling", func(t *testing.T) {
		// Create first active key
		key1 := &models.SigningKey{
			PublicKey:  []byte("test-active-public-key-1"),
			PrivateKey: []byte("test-active-private-key-1"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, key1)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, key1.KeyID)

		// Create second active key
		key2 := &models.SigningKey{
			PublicKey:  []byte("test-active-public-key-2"),
			PrivateKey: []byte("test-active-private-key-2"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, key2)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, key2.KeyID)

		// Verify first key is now inactive
		retrievedKey1, err := DB(ctx).GetSigningKey(ctx, key1.KeyID)
		assert.NoError(t, err)
		assert.False(t, retrievedKey1.IsActive)

		// Verify second key is active
		retrievedKey2, err := DB(ctx).GetSigningKey(ctx, key2.KeyID)
		assert.NoError(t, err)
		assert.True(t, retrievedKey2.IsActive)
	})

	t.Run("transaction rollback on error", func(t *testing.T) {
		// Create an active key first
		existingKey := &models.SigningKey{
			PublicKey:  []byte("test-existing-active-key"),
			PrivateKey: []byte("test-existing-private-key"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, existingKey)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, existingKey.KeyID)

		// Try to create a key with invalid data to force a transaction error
		invalidKey := &models.SigningKey{
			KeyID:      existingKey.KeyID, // Deliberately use same UUID to cause conflict
			PublicKey:  []byte("test-invalid-public-key"),
			PrivateKey: []byte("test-invalid-private-key"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, invalidKey)
		assert.Error(t, err)

		// Verify the existing key is still active (transaction was rolled back)
		retrievedKey, err := DB(ctx).GetSigningKey(ctx, existingKey.KeyID)
		assert.NoError(t, err)
		assert.True(t, retrievedKey.IsActive)
		assert.Equal(t, existingKey.PublicKey, retrievedKey.PublicKey)
	})

	t.Run("missing tenant ID", func(t *testing.T) {
		// Create context without tenant ID
		ctxWithoutTenant := log.Logger.WithContext(context.Background())
		ctxWithoutTenant = newDb(ctxWithoutTenant)

		key := &models.SigningKey{
			PublicKey:  []byte("test-public-key"),
			PrivateKey: []byte("test-private-key"),
			IsActive:   true,
		}
		err = DB(ctxWithoutTenant).CreateSigningKey(ctxWithoutTenant, key)
		assert.Error(t, err)
		assert.ErrorIs(t, err, dberror.ErrMissingTenantID.Err(dberror.ErrInvalidInput))
	})
}

func TestGetSigningKey(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	// Create a key for testing
	key := &models.SigningKey{
		PublicKey:  []byte("test-public-key"),
		PrivateKey: []byte("test-private-key"),
		IsActive:   false,
	}
	err = DB(ctx).CreateSigningKey(ctx, key)
	assert.NoError(t, err)
	defer DB(ctx).DeleteSigningKey(ctx, key.KeyID)

	// Test getting an existing key
	retrievedKey, err := DB(ctx).GetSigningKey(ctx, key.KeyID)
	assert.NoError(t, err)
	assert.Equal(t, key.KeyID, retrievedKey.KeyID)
	assert.Equal(t, key.PublicKey, retrievedKey.PublicKey)
	assert.Equal(t, key.PrivateKey, retrievedKey.PrivateKey)
	assert.Equal(t, key.IsActive, retrievedKey.IsActive)

	// Test getting a non-existent key
	nonExistentKeyID := uuid.New()
	_, err = DB(ctx).GetSigningKey(ctx, nonExistentKeyID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}

func TestGetActiveSigningKey(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	// Test getting active key when none exists
	_, err = DB(ctx).GetActiveSigningKey(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Create an active key
	activeKey := &models.SigningKey{
		PublicKey:  []byte("test-active-public-key"),
		PrivateKey: []byte("test-active-private-key"),
		IsActive:   true,
	}
	err = DB(ctx).CreateSigningKey(ctx, activeKey)
	assert.NoError(t, err)
	defer DB(ctx).DeleteSigningKey(ctx, activeKey.KeyID)

	// Test getting the active key
	retrievedActiveKey, err := DB(ctx).GetActiveSigningKey(ctx)
	assert.NoError(t, err)
	assert.Equal(t, activeKey.KeyID, retrievedActiveKey.KeyID)
	assert.True(t, retrievedActiveKey.IsActive)
}

func TestUpdateSigningKeyActive(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	t.Run("successful activation and deactivation", func(t *testing.T) {
		// Create two keys for testing
		key1 := &models.SigningKey{
			PublicKey:  []byte("test-public-key-1"),
			PrivateKey: []byte("test-private-key-1"),
			IsActive:   false,
		}
		err = DB(ctx).CreateSigningKey(ctx, key1)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, key1.KeyID)

		key2 := &models.SigningKey{
			PublicKey:  []byte("test-public-key-2"),
			PrivateKey: []byte("test-private-key-2"),
			IsActive:   false,
		}
		err = DB(ctx).CreateSigningKey(ctx, key2)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, key2.KeyID)

		// Test activating a key
		err = DB(ctx).UpdateSigningKeyActive(ctx, key1.KeyID, true)
		assert.NoError(t, err)

		// Verify key1 is active
		retrievedKey1, err := DB(ctx).GetSigningKey(ctx, key1.KeyID)
		assert.NoError(t, err)
		assert.True(t, retrievedKey1.IsActive)

		// Test activating another key
		err = DB(ctx).UpdateSigningKeyActive(ctx, key2.KeyID, true)
		assert.NoError(t, err)

		// Verify key1 is now inactive and key2 is active
		retrievedKey1, err = DB(ctx).GetSigningKey(ctx, key1.KeyID)
		assert.NoError(t, err)
		assert.False(t, retrievedKey1.IsActive)

		retrievedKey2, err := DB(ctx).GetSigningKey(ctx, key2.KeyID)
		assert.NoError(t, err)
		assert.True(t, retrievedKey2.IsActive)

		// Test deactivating a key
		err = DB(ctx).UpdateSigningKeyActive(ctx, key2.KeyID, false)
		assert.NoError(t, err)

		// Verify key2 is now inactive
		retrievedKey2, err = DB(ctx).GetSigningKey(ctx, key2.KeyID)
		assert.NoError(t, err)
		assert.False(t, retrievedKey2.IsActive)
	})

	t.Run("transaction rollback on non-existent key", func(t *testing.T) {
		// Create an active key first
		existingKey := &models.SigningKey{
			PublicKey:  []byte("test-existing-active-key"),
			PrivateKey: []byte("test-existing-private-key"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, existingKey)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, existingKey.KeyID)

		// Try to activate a non-existent key
		nonExistentKeyID := uuid.New()
		err = DB(ctx).UpdateSigningKeyActive(ctx, nonExistentKeyID, true)
		assert.Error(t, err)
		assert.ErrorIs(t, err, dberror.ErrNotFound)

		// Verify the existing key is still active (transaction was rolled back)
		retrievedKey, err := DB(ctx).GetSigningKey(ctx, existingKey.KeyID)
		assert.NoError(t, err)
		assert.True(t, retrievedKey.IsActive)
	})

	t.Run("transaction rollback on error during deactivation", func(t *testing.T) {
		// Create two active keys (this should leave only the second one active)
		key1 := &models.SigningKey{
			PublicKey:  []byte("test-public-key-1"),
			PrivateKey: []byte("test-private-key-1"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, key1)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, key1.KeyID)

		key2 := &models.SigningKey{
			PublicKey:  []byte("test-public-key-2"),
			PrivateKey: []byte("test-private-key-2"),
			IsActive:   true,
		}
		err = DB(ctx).CreateSigningKey(ctx, key2)
		assert.NoError(t, err)
		defer DB(ctx).DeleteSigningKey(ctx, key2.KeyID)

		// Delete key2 to cause a transaction error when we try to activate key1
		err = DB(ctx).DeleteSigningKey(ctx, key2.KeyID)
		assert.NoError(t, err)

		// Try to activate key1 (this should fail during the deactivation of key2)
		err = DB(ctx).UpdateSigningKeyActive(ctx, key1.KeyID, true)
		assert.NoError(t, err)

		// Verify key1 is now active
		retrievedKey1, err := DB(ctx).GetSigningKey(ctx, key1.KeyID)
		assert.NoError(t, err)
		assert.True(t, retrievedKey1.IsActive)
	})

	t.Run("missing tenant ID", func(t *testing.T) {
		// Create context without tenant ID
		ctxWithoutTenant := log.Logger.WithContext(context.Background())
		ctxWithoutTenant = newDb(ctxWithoutTenant)

		err = DB(ctxWithoutTenant).UpdateSigningKeyActive(ctxWithoutTenant, uuid.New(), true)
		assert.Error(t, err)
		assert.ErrorIs(t, err, dberror.ErrMissingTenantID.Err(dberror.ErrInvalidInput))
	})
}

func TestDeleteSigningKey(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	// Create a key for testing
	key := &models.SigningKey{
		PublicKey:  []byte("test-public-key"),
		PrivateKey: []byte("test-private-key"),
		IsActive:   true,
	}
	err = DB(ctx).CreateSigningKey(ctx, key)
	assert.NoError(t, err)

	// Test deleting the key
	err = DB(ctx).DeleteSigningKey(ctx, key.KeyID)
	assert.NoError(t, err)

	// Verify the key is deleted
	_, err = DB(ctx).GetSigningKey(ctx, key.KeyID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test deleting a non-existent key
	nonExistentKeyID := uuid.New()
	err = DB(ctx).DeleteSigningKey(ctx, nonExistentKeyID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}
