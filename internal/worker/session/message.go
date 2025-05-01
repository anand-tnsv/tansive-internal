package session

import (
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/jsonrpc"
)

// Message Error Codes
const (
	ErrorUnexpectedMessageType = 4409 // Unexpected message type received
	ErrorTimeout               = 4504 // Timeout error, e.g., for heartbeat or response waiting
)

const (
	InitChannelMessageType jsonrpc.MethodType = "init"
	HeartbeatMessageType   jsonrpc.MethodType = "heartbeat"
)

type InitChannelMessage struct {
	SessionId         uuid.UUID `json:"session_id"`         // Unique identifier for the session.
	HeartbeatInterval int64     `json:"heartbeat_interval"` // Heartbeat interval in seconds.
}

type HeartbeatMessage struct {
	SessionId uuid.UUID `json:"session_id"` // Unique identifier for the session.
}
