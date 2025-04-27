package api

import (
	"github.com/tansive/tansive-internal/pkg/types"
)

type Session struct {
	ID      string            `json:"id"`
	Context types.NullableAny `json:"context"`
}

type CreateSessionRequest struct {
	Session
}

type CreateSessionResponse struct {
	ID string `json:"id"`
}

type GetSessionResponse struct {
	Session
}

type ListSessionsResponse struct {
	Sessions []Session `json:"sessions"`
}
