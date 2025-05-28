package auth

type TokenType string

const (
	IdentityTokenType TokenType = "id"
	AccessTokenType   TokenType = "access"
	UnknownTokenType  TokenType = "unknown"
)

type TokenVersion string

const (
	TokenVersionV0_1 TokenVersion = "0.1"
)
