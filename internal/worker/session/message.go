package session

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/jsonrpc"
)

// Message Error Codes
const (
	ErrorUnexpectedMessageType = 4409 // Unexpected message type received
	ErrorTimeout               = 4504 // Timeout error, e.g., for heartbeat or response waiting
)

const (
	InitChannel jsonrpc.MethodType = "init"
	Heartbeat   jsonrpc.MethodType = "heartbeat"
	StartPty    jsonrpc.MethodType = "start_pty"
	StopPty     jsonrpc.MethodType = "stop_pty"
	PtyData     jsonrpc.MethodType = "pty_data"
)

type InitChannelMessage struct {
	SessionId         uuid.UUID `json:"session_id"`         // Unique identifier for the session.
	HeartbeatInterval int64     `json:"heartbeat_interval"` // Heartbeat interval in seconds.
}

type HeartbeatMessage struct {
	SessionId uuid.UUID `json:"session_id"` // Unique identifier for the session.
}

type StartPtyMessage struct {
	SessionId  uuid.UUID       `json:"session_id"`        // Unique identifier for the session.
	TerminalId uuid.UUID       `json:"terminal_id"`       // Unique identifier for the terminal.
	Context    json.RawMessage `json:"context,omitempty"` // Context for the PTY session, can be any JSON-serializable data.
	Marker     uuid.UUID       `json:"marker,omitempty"`  // data marker, used for buffered resends
}

type StopPtyMessage struct {
	SessionId  uuid.UUID `json:"session_id"`  // Unique identifier for the session.
	TerminalId uuid.UUID `json:"terminal_id"` // Unique identifier for the terminal.
}

type PtyDataMessage struct {
	SessionId  uuid.UUID `json:"session_id"`  // Unique identifier for the session.
	TerminalId uuid.UUID `json:"terminal_id"` // Unique identifier for the terminal.
	Marker     uuid.UUID `json:"marker"`      // data marker, used for buffered resends
	Data       string    `json:"data"`        // Data to be sent to the PTY, can be any JSON-serializable data.
}
