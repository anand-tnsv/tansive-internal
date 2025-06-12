package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/tansive/tansive-internal/internal/common/uuid"
)

// This is a PKCE flow implementation in-memory for single instance deployment. This will need to move in to a
// distributed cache for multi-instance deployment.

type AuthCodeMetadata struct {
	SessionID     uuid.UUID
	Code          string
	CodeChallenge string
	ExpiresAt     time.Time
}

var (
	authCodes = make(map[string]AuthCodeMetadata)
	mu        sync.RWMutex
)

func generateRandomCode(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func CreateAuthCode(ctx context.Context, sessionID uuid.UUID, codeChallenge string) (string, error) {
	code, err := generateRandomCode(32) // 32 bytes -> 43-char base64 string
	if err != nil {
		return "", err
	}

	mu.Lock()
	authCodes[code] = AuthCodeMetadata{
		SessionID:     sessionID,
		Code:          code,
		CodeChallenge: codeChallenge,
		ExpiresAt:     time.Now().Add(10 * time.Minute),
	}
	mu.Unlock()
	return code, nil
}

func GetAuthCode(ctx context.Context, code, codeVerifier string) (AuthCodeMetadata, error) {
	mu.Lock()
	defer mu.Unlock()

	authCode, ok := authCodes[code]
	if !ok {
		return AuthCodeMetadata{}, errors.New("invalid auth code")
	}
	defer delete(authCodes, code)

	if time.Now().After(authCode.ExpiresAt) {
		return AuthCodeMetadata{}, errors.New("auth code expired")
	}

	hashed := sha256.Sum256([]byte(codeVerifier))
	expectedChallenge := base64.RawURLEncoding.EncodeToString(hashed[:])

	if authCode.CodeChallenge != expectedChallenge {
		return AuthCodeMetadata{}, errors.New("invalid code verifier")
	}

	return authCode, nil
}
