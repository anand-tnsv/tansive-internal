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
	MethodInitChannel   jsonrpc.MethodType = "init"
	MethodHeartbeat     jsonrpc.MethodType = "heartbeat"
	MethodStartTerminal jsonrpc.MethodType = "startTerminal"
	MethodStopTerminal  jsonrpc.MethodType = "stopTerminal"
	MethodTerminalData  jsonrpc.MethodType = "terminalData"
)

type InitChannelNotification struct {
	SessionId         uuid.UUID `json:"session_id"`         // Unique identifier for the session.
	HeartbeatInterval int64     `json:"heartbeat_interval"` // Heartbeat interval in seconds.
}

type HeartbeatNotification struct {
	SessionId uuid.UUID `json:"session_id"` // Unique identifier for the session.
}

type StartTerminalRequest struct {
	SessionId  uuid.UUID `json:"session_id"`       // Unique identifier for the session.
	TerminalId uuid.UUID `json:"terminal_id"`      // Unique identifier for the terminal.
	Marker     uuid.UUID `json:"marker,omitempty"` // data marker, used for buffered resends
}

type StopTerminalRequest struct {
	SessionId  uuid.UUID `json:"session_id"`  // Unique identifier for the session.
	TerminalId uuid.UUID `json:"terminal_id"` // Unique identifier for the terminal.
}

type TerminalDataNotification struct {
	SessionId  uuid.UUID `json:"session_id"`     // Unique identifier for the session.
	TerminalId uuid.UUID `json:"terminal_id"`    // Unique identifier for the terminal.
	Marker     string    `json:"marker"`         // data marker, used for buffered resends
	Data       string    `json:"data,omitempty"` // Data to be sent to the PTY, can be any JSON-serializable data.
}
