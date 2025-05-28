package auth

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth/keymanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	keyManagerInstance *keymanager.KeyManager
	keyManagerOnce     sync.Once
)

// Token represents a JWT token with its associated claims and validation methods
type Token struct {
	token  *jwt.Token
	claims jwt.MapClaims
	view   *models.View
}

func getKeyManager() *keymanager.KeyManager {
	keyManagerOnce.Do(func() {
		keyManagerInstance = keymanager.NewKeyManager()
	})
	return keyManagerInstance
}

// NewToken creates a new Token instance from a JWT token string
func NewToken(ctx context.Context, tokenString string) (*Token, apperrors.Error) {
	signingKey, err := getKeyManager().GetActiveKey(ctx)
	if err != nil {
		return nil, err
	}

	var token *jwt.Token
	var parseErr error
	token, parseErr = jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		// Validate the signing method
		if _, ok := token.Method.(*jwt.SigningMethodEd25519); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return signingKey.PublicKey, nil
	})

	if parseErr != nil {
		log.Ctx(ctx).Error().Err(parseErr).Msg("failed to parse token")
		return nil, ErrUnableToGenerateToken.Err(parseErr)
	}

	if !token.Valid {
		return nil, ErrUnableToGenerateToken
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrUnableToGenerateToken
	}

	view_id, ok := claims["view_id"].(string)
	if !ok {
		return nil, ErrUnableToGenerateToken
	}

	viewID, parseUUIDErr := uuid.Parse(view_id)
	if parseUUIDErr != nil {
		return nil, ErrUnableToGenerateToken.Err(parseUUIDErr)
	}

	tenantID, ok := claims["tenant_id"].(string)
	if !ok {
		return nil, ErrUnableToGenerateToken
	}

	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))

	view, err := db.DB(ctx).GetView(ctx, viewID)
	if err != nil {
		return nil, err
	}

	return &Token{
		token:  token,
		claims: claims,
		view:   view,
	}, nil
}

// isTokenRevoked checks if a token has been revoked by its JWT ID (jti)
// Currently a dummy implementation that always returns false
func isTokenRevoked(jti string) bool {
	_ = jti
	return false
}

// Validate checks if the token is valid and not expired
func (t *Token) Validate() bool {
	if t.token == nil || !t.token.Valid {
		return false
	}

	now := time.Now()

	// Check if token is expired
	exp, ok := t.claims["exp"].(float64)
	if !ok {
		return false
	}
	if time.Unix(int64(exp), 0).Before(now) {
		return false
	}

	// Check if token is not yet valid (nbf)
	if nbf, ok := t.claims["nbf"].(float64); ok {
		if time.Unix(int64(nbf), 0).After(now) {
			return false
		}
	}

	// Check if token was issued too far in the past (iat)
	if iat, ok := t.claims["iat"].(float64); ok {
		// Reject tokens issued more than 24 hours ago
		if time.Unix(int64(iat), 0).Before(now.Add(-24 * time.Hour)) {
			return false
		}
	}

	// Check required claims
	requiredClaims := []string{"view_id", "tenant_id", "iss", "aud", "jti"}
	for _, claim := range requiredClaims {
		if _, ok := t.claims[claim]; !ok {
			return false
		}
	}

	// Check if token has been revoked using its JWT ID
	jti, ok := t.claims["jti"].(string)
	if !ok {
		return false
	}
	if isTokenRevoked(jti) {
		return false
	}

	return true
}

// Get retrieves a claim value from the token
func (t *Token) Get(key string) (any, bool) {
	if t.claims == nil {
		return nil, false
	}
	val, ok := t.claims[key]
	return val, ok
}

// GetString retrieves a string claim value from the token
func (t *Token) GetString(key string) (string, bool) {
	val, ok := t.Get(key)
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

func (t *Token) GetTokenUse() TokenType {
	tokenType, ok := t.Get("token_use")
	if !ok {
		return UnknownTokenType
	}
	return TokenType(tokenType.(string))
}

func (t *Token) GetSubject() string {
	subject, ok := t.Get("sub")
	if !ok {
		return ""
	}
	return subject.(string)
}

func (t *Token) GetTenantID() string {
	tenantID, ok := t.Get("tenant_id")
	if !ok {
		return ""
	}
	return tenantID.(string)
}

// GetUUID retrieves a UUID claim value from the token
func (t *Token) GetUUID(key string) (uuid.UUID, bool) {
	str, ok := t.GetString(key)
	if !ok {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(str)
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// GetViewID returns the view ID associated with the token
func (t *Token) GetViewID() uuid.UUID {
	if t.view == nil {
		return uuid.Nil
	}
	return t.view.ViewID
}

// GetExpiry returns the token's expiration time
func (t *Token) GetExpiry() time.Time {
	exp, ok := t.claims["exp"].(float64)
	if !ok {
		return time.Time{}
	}
	return time.Unix(int64(exp), 0)
}

// GetView returns the view associated with the token
func (t *Token) GetView() *models.View {
	return t.view
}

// GetRawToken returns the raw token string
func (t *Token) GetRawToken() string {
	if t.token == nil {
		return ""
	}
	return t.token.Raw
}
