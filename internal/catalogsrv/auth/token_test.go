package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewToken(t *testing.T) {
	ctx, _, _, viewID, _, _ := setupTest(t)

	claims := jwt.MapClaims{
		"view_id":   viewID.String(),
		"tenant_id": "TABCDE",
		"iss":       "test-issuer",
		"aud":       "test-audience",
		"jti":       uuid.New().String(),
		"exp":       time.Now().Add(time.Hour).Unix(),
		"iat":       time.Now().Unix(),
		"nbf":       time.Now().Unix(),
		"sub":       "test-subject",
		"token_use": "access",
	}

	signingKey, err := getKeyManager().GetActiveKey(ctx)
	require.NoError(t, err)
	require.NotNil(t, signingKey)

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	var tokenString string
	var goerr error
	tokenString, goerr = token.SignedString(signingKey.PrivateKey)
	if goerr != nil {
		err = ErrUnableToGenerateToken.MsgErr("unable to sign token", goerr)
	}
	require.NoError(t, err)

	tests := []struct {
		name        string
		tokenString string
		wantErr     bool
	}{
		{
			name:        "Valid token",
			tokenString: tokenString,
			wantErr:     false,
		},
		{
			name:        "Invalid token string",
			tokenString: "invalid.token.string",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := NewToken(ctx, tt.tokenString)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, token)
		})
	}
}

func TestTokenValidation(t *testing.T) {
	ctx, _, _, viewID, _, _ := setupTest(t)
	now := time.Now()

	tests := []struct {
		name    string
		claims  jwt.MapClaims
		isValid bool
	}{
		{
			name: "Valid token",
			claims: jwt.MapClaims{
				"view_id":   viewID.String(),
				"tenant_id": "TABCDE",
				"iss":       "test-issuer",
				"aud":       "test-audience",
				"jti":       uuid.New().String(),
				"exp":       now.Add(time.Hour).Unix(),
				"iat":       now.Unix(),
				"nbf":       now.Unix(),
			},
			isValid: true,
		},
		{
			name: "Expired token",
			claims: jwt.MapClaims{
				"view_id":   viewID.String(),
				"tenant_id": "TABCDE",
				"iss":       "test-issuer",
				"aud":       "test-audience",
				"jti":       uuid.New().String(),
				"exp":       now.Add(-time.Hour).Unix(),
				"iat":       now.Unix(),
				"nbf":       now.Unix(),
			},
			isValid: false,
		},
		{
			name: "Token with missing required claims",
			claims: jwt.MapClaims{
				"view_id": viewID.String(),
				"exp":     now.Add(time.Hour).Unix(),
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signingKey, err := getKeyManager().GetActiveKey(ctx)
			require.NoError(t, err)
			require.NotNil(t, signingKey)

			token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, tt.claims)
			var tokenString string
			var goerr error
			tokenString, goerr = token.SignedString(signingKey.PrivateKey)
			if goerr != nil {
				err = ErrUnableToGenerateToken.MsgErr("unable to sign token", goerr)
			}
			require.NoError(t, err)

			parsedToken, err := NewToken(ctx, tokenString)
			if !tt.isValid {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.isValid, parsedToken.Validate())
		})
	}
}

func TestTokenGetters(t *testing.T) {
	ctx, _, _, viewID, _, _ := setupTest(t)
	subject := "test-subject"
	tenantID := "TABCDE"
	tokenUse := "access"

	claims := jwt.MapClaims{
		"sub":       subject,
		"tenant_id": tenantID,
		"token_use": tokenUse,
		"view_id":   viewID.String(),
	}

	signingKey, err := getKeyManager().GetActiveKey(ctx)
	require.NoError(t, err)
	require.NotNil(t, signingKey)

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	var tokenString string
	var goerr error
	tokenString, goerr = token.SignedString(signingKey.PrivateKey)
	if goerr != nil {
		err = ErrUnableToGenerateToken.MsgErr("unable to sign token", goerr)
	}
	require.NoError(t, err)

	parsedToken, err := NewToken(ctx, tokenString)
	require.NoError(t, err)

	t.Run("GetSubject", func(t *testing.T) {
		assert.Equal(t, subject, parsedToken.GetSubject())
	})

	t.Run("GetTenantID", func(t *testing.T) {
		assert.Equal(t, tenantID, parsedToken.GetTenantID())
	})

	t.Run("GetTokenUse", func(t *testing.T) {
		assert.Equal(t, TokenType(tokenUse), parsedToken.GetTokenUse())
	})

	t.Run("GetViewID", func(t *testing.T) {
		assert.Equal(t, viewID, parsedToken.GetViewID())
	})

	t.Run("GetView", func(t *testing.T) {
		assert.Equal(t, viewID, parsedToken.GetView().ViewID)
	})

	t.Run("GetUUID", func(t *testing.T) {
		id, ok := parsedToken.GetUUID("view_id")
		assert.True(t, ok)
		assert.Equal(t, viewID, id)
	})
}
