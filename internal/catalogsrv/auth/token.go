package auth

import (
	"context"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

// Token represents a JWT token with its associated claims and validation methods
type Token struct {
	token  *jwt.Token
	claims jwt.MapClaims
	view   *models.View
}

// NewToken creates a new Token instance from a JWT token string
func NewToken(ctx context.Context, tokenString string) (*Token, apperrors.Error) {
	// Get the active signing key
	signingKey, err := getActiveSigningKey(ctx)
	if err != nil {
		return nil, err
	}

	// Parse and validate the token
	var token *jwt.Token
	var parseErr error
	token, parseErr = jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
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

	// Extract claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrUnableToGenerateToken
	}

	// Get the view ID from claims
	view_id, ok := claims["view_id"].(string)
	if !ok {
		return nil, ErrUnableToGenerateToken
	}

	// Parse the view ID
	viewID, parseUUIDErr := uuid.Parse(view_id)
	if parseUUIDErr != nil {
		return nil, ErrUnableToGenerateToken.Err(parseUUIDErr)
	}

	// Get the tenant ID
	tenantID, ok := claims["tenant_id"].(string)
	if !ok {
		return nil, ErrUnableToGenerateToken
	}

	ctx = catcommon.WithTenantID(ctx, catcommon.TenantId(tenantID))

	// Get the view from database
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

// Validate checks if the token is valid and not expired
func (t *Token) Validate() bool {
	if t.token == nil || !t.token.Valid {
		return false
	}

	// Check if token is expired
	exp, ok := t.claims["exp"].(float64)
	if !ok {
		return false
	}

	return time.Unix(int64(exp), 0).After(time.Now())
}

// Get retrieves a claim value from the token
func (t *Token) Get(key string) (interface{}, bool) {
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

func (t *Token) GetTokenType() catcommon.TokenType {
	tokenType, ok := t.Get("token_type")
	if !ok {
		return catcommon.TokenTypeUnknown
	}
	return catcommon.TokenType(tokenType.(string))
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
