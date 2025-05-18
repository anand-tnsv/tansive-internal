package catalogmanager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

func CreateToken(ctx context.Context, derivedView *ViewDefinition, viewId uuid.UUID) (string, apperrors.Error) {
	// get current view from context
	creatorView := getViewFromContext(ctx)
	if creatorView == nil {
		return "", ErrUnauthorizedToCreateView
	}

	// check if derived view can be created from creator view
	if err := ValidateDerivedView(ctx, creatorView, derivedView); err != nil {
		return "", err
	}

	type viewClaims struct {
		jwt.RegisteredClaims
	}

	// create a signed jwt token
	tokenDuration, err := config.ParseTokenDuration(config.Config().DefaultTokenValidity)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to parse token duration")
		return "", ErrUnableToParseTokenDuration
	}

	tokenExpiry := time.Now().Add(tokenDuration)

	v := &models.ViewToken{
		ViewID:   viewId,
		ExpireAt: tokenExpiry,
	}
	if err := db.DB(ctx).CreateViewToken(ctx, v); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to create view token")
		return "", ErrUnableToCreateView
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, viewClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   viewId.String(),
			Issuer:    config.Config().ServerHostName + ":" + config.Config().ServerPort,
			ExpiresAt: jwt.NewNumericDate(tokenExpiry),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Audience:  []string{config.Config().ServerHostName + ":" + config.Config().ServerPort},
			ID:        v.TokenID.String(),
		},
	})

	signingKey, apperr := getActiveSigningKey(ctx)
	if apperr != nil {
		log.Ctx(ctx).Error().Err(apperr).Msg("unable to get active signing key")
		return "", apperr
	}

	tokenString, err := token.SignedString(signingKey.PrivateKey)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to sign token")
		return "", ErrUnableToGenerateToken
	}

	return tokenString, nil
}

type viewKeyType string

const viewKey viewKeyType = "TansiveView"

func addViewToContext(ctx context.Context, viewDefinition *ViewDefinition) context.Context {
	return context.WithValue(ctx, viewKey, viewDefinition)
}

func getViewFromContext(ctx context.Context) *ViewDefinition {
	v, ok := ctx.Value(viewKey).(*ViewDefinition)
	if !ok {
		return nil
	}
	return v
}

type signingKey struct {
	KeyID      uuid.UUID
	PrivateKey ed25519.PrivateKey
	PublicKey  ed25519.PublicKey
	ExpiresAt  time.Time
}

func (sk *signingKey) IsExpired() bool {
	return sk.ExpiresAt.Before(time.Now())
}

var activeSigningKey *signingKey
var activeSigningKeyMutex sync.Mutex

// WARNING: FOR LOCAL DEVELOPMENT ONLY
// This signing key management is NOT suitable for production use.
// Production environments should use a proper KMS (Key Management Service).

// getActiveSigningKey retrieves or creates a signing key for local development.
// This implementation is NOT suitable for production use and should be replaced
// with proper KMS integration in production environments.
func getActiveSigningKey(ctx context.Context) (*signingKey, apperrors.Error) {
	if activeSigningKey != nil {
		return activeSigningKey, nil
	}

	return retrieveOrCreateSigningKey(ctx)
}

func retrieveOrCreateSigningKey(ctx context.Context) (*signingKey, apperrors.Error) {
	activeSigningKeyMutex.Lock()
	defer activeSigningKeyMutex.Unlock()

	if activeSigningKey != nil {
		return activeSigningKey, nil
	}

	key, err := db.DB(ctx).GetActiveSigningKey(ctx)
	if err != nil {
		if !errors.Is(err, dberror.ErrNotFound) {
			return nil, err
		}
	}

	if key == nil {
		// Create new key pair
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("unable to generate signing key")
			return nil, ErrUnableToGenerateSigningKey
		}
		encKey, err := common.Encrypt(priv, config.Config().KeyEncryptionPasswd)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("unable to encrypt signing key")
			return nil, ErrUnableToGenerateSigningKey
		}
		key = &models.SigningKey{
			PublicKey:  pub,
			PrivateKey: encKey,
			IsActive:   true,
		}
		if err := db.DB(ctx).CreateSigningKey(ctx, key); err != nil {
			return nil, err
		}
		activeSigningKey = &signingKey{
			KeyID:      key.KeyID,
			PrivateKey: priv,
			PublicKey:  pub,
		}
	} else {
		// Decrypt the existing key
		decKey, err := common.Decrypt(key.PrivateKey, config.Config().KeyEncryptionPasswd)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("unable to decrypt signing key")
			return nil, ErrUnableToGenerateSigningKey
		}
		activeSigningKey = &signingKey{
			KeyID:      key.KeyID,
			PrivateKey: decKey,
			PublicKey:  key.PublicKey,
		}
	}

	return activeSigningKey, nil
}
