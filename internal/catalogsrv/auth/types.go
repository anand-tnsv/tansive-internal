package auth

type TokenType string

const (
	IdentityTokenType TokenType = "id"
	AccessTokenType   TokenType = "access"
	UnknownTokenType  TokenType = "unknown"
)
