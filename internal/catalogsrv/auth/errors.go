package auth

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

// Base auth error
var (
	ErrAuth apperrors.Error = apperrors.New("auth error").SetStatusCode(http.StatusInternalServerError)
)

// Not found errors
var (
	ErrCatalogNotFound apperrors.Error = ErrAuth.New("catalog not found").SetStatusCode(http.StatusNotFound)
	ErrViewNotFound    apperrors.Error = ErrAuth.New("view not found").SetStatusCode(http.StatusNotFound)
)

// Validation errors
var (
	ErrInvalidView    apperrors.Error = ErrAuth.New("invalid view").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCatalog apperrors.Error = ErrAuth.New("invalid catalog").SetStatusCode(http.StatusBadRequest)
	ErrInvalidRequest apperrors.Error = ErrAuth.New("invalid request").SetStatusCode(http.StatusBadRequest)
)

// Authorization errors
var (
	ErrUnauthorized apperrors.Error = ErrAuth.New("unauthorized access").SetStatusCode(http.StatusUnauthorized)
)

// Token errors
var (
	ErrTokenGeneration            apperrors.Error = ErrAuth.New("failed to generate token").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToGenerateSigningKey apperrors.Error = ErrAuth.New("unable to generate signing key").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToParseTokenDuration apperrors.Error = ErrAuth.New("unable to parse token duration").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToGenerateToken      apperrors.Error = ErrAuth.New("unable to generate token").SetStatusCode(http.StatusInternalServerError)
)

// Ops errors
var (
	ErrUnableToCreateView apperrors.Error = ErrAuth.New("unable to create view").SetStatusCode(http.StatusInternalServerError)
)
