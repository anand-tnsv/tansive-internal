package catalogmanager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type createTokenOptions struct {
	parentViewId      uuid.UUID
	parentView        *types.ViewDefinition
	createDerivedView bool
	additionalClaims  map[string]any
}

type createTokenOption func(*createTokenOptions)

func WithParentViewId(id uuid.UUID) createTokenOption {
	return func(o *createTokenOptions) {
		o.parentViewId = id
	}
}

func WithParentViewDefinition(view *types.ViewDefinition) createTokenOption {
	return func(o *createTokenOptions) {
		o.parentView = view
	}
}

func WithAdditionalClaims(claims map[string]any) createTokenOption {
	if claims == nil {
		claims = make(map[string]any)
	}
	return func(o *createTokenOptions) {
		o.additionalClaims = claims
	}
}

func CreateDerivedView() createTokenOption {
	return func(o *createTokenOptions) {
		o.createDerivedView = true
	}
}

func CreateToken(ctx context.Context, derivedView *models.View, opts ...createTokenOption) (string, time.Time, apperrors.Error) {
	options := &createTokenOptions{}
	for _, opt := range opts {
		opt(options)
	}
	tokenExpiry := time.Time{}
	var parentViewDef *types.ViewDefinition
	// get parent view from database
	if options.parentView != nil {
		parentViewDef = options.parentView
		if parentViewDef == nil {
			return "", tokenExpiry, ErrUnableToCreateView
		}
	} else {
		var p *models.View
		var err apperrors.Error
		if options.parentViewId != uuid.Nil {
			p, err = db.DB(ctx).GetView(ctx, options.parentViewId)
			if err != nil {
				return "", tokenExpiry, err
			}
			parentViewDef = &types.ViewDefinition{}
			if err := json.Unmarshal(p.Rules, &parentViewDef); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("unable to unmarshal parent view")
				return "", tokenExpiry, ErrUnableToCreateView
			}
		} else {
			return "", tokenExpiry, ErrUnableToCreateView
		}
	}

	derivedViewDef := &types.ViewDefinition{}
	if err := json.Unmarshal(derivedView.Rules, &derivedViewDef); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to unmarshal derived view")
		return "", tokenExpiry, ErrUnableToCreateView
	}

	// check if derived view can be created from creator view
	if err := ValidateDerivedView(ctx, parentViewDef, derivedViewDef); err != nil {
		return "", tokenExpiry, err
	}

	if options.createDerivedView {
		if err := db.DB(ctx).CreateView(ctx, derivedView); err != nil {
			return "", tokenExpiry, err
		}
	}

	// create a signed jwt token
	tokenDuration, errif := config.ParseTokenDuration(config.Config().DefaultTokenValidity)
	if errif != nil {
		log.Ctx(ctx).Error().Err(errif).Msg("unable to parse token duration")
		return "", tokenExpiry, ErrUnableToParseTokenDuration
	}

	tokenExpiry = time.Now().Add(tokenDuration)

	v := &models.ViewToken{
		ViewID:   derivedView.ViewID,
		ExpireAt: tokenExpiry,
	}
	if err := db.DB(ctx).CreateViewToken(ctx, v); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to create view token")
		return "", tokenExpiry, ErrUnableToCreateView
	}

	claims := jwt.MapClaims{
		"view_id":   derivedView.ViewID.String(),
		"tenant_id": catcommon.TenantIdFromContext(ctx),
		"iss":       config.Config().ServerHostName + ":" + config.Config().ServerPort,
		"exp":       jwt.NewNumericDate(tokenExpiry),
		"iat":       jwt.NewNumericDate(time.Now()),
		"aud":       []string{config.Config().ServerHostName + ":" + config.Config().ServerPort},
		"jti":       v.TokenID.String(),
	}

	for k, v := range options.additionalClaims {
		claims[k] = v
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)

	signingKey, apperr := getActiveSigningKey(ctx)
	if apperr != nil {
		log.Ctx(ctx).Error().Err(apperr).Msg("unable to get active signing key")
		return "", tokenExpiry, apperr
	}

	tokenString, errif := token.SignedString(signingKey.PrivateKey)
	if errif != nil {
		log.Ctx(ctx).Error().Err(errif).Msg("unable to sign token")
		return "", tokenExpiry, ErrUnableToGenerateToken
	}

	return tokenString, tokenExpiry, nil
}

type viewKeyType string

const viewKey viewKeyType = "TansiveView"

func addViewToContext(ctx context.Context, viewDefinition *types.ViewDefinition) context.Context {
	return context.WithValue(ctx, viewKey, viewDefinition)
}

func getViewFromContext(ctx context.Context) *types.ViewDefinition {
	v, ok := ctx.Value(viewKey).(*types.ViewDefinition)
	if !ok {
		return nil
	}
	return v
}

var _ = getViewFromContext
var _ = addViewToContext

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
		encKey, err := catcommon.Encrypt(priv, config.Config().KeyEncryptionPasswd)
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
		decKey, err := catcommon.Decrypt(key.PrivateKey, config.Config().KeyEncryptionPasswd)
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
