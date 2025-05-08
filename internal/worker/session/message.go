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
	MethodInitChannel   jsonrpc.MethodType = "channel.init"
	MethodHeartbeat     jsonrpc.MethodType = "channel.heartbeat"
	MethodStartTerminal jsonrpc.MethodType = "terminal.start"
	MethodStopTerminal  jsonrpc.MethodType = "terminal.stop"
	MethodTerminalData  jsonrpc.MethodType = "terminal.data"
	MethodRunCommand    jsonrpc.MethodType = "command.run"
	MethodCommandData   jsonrpc.MethodType = "command.data"
	MethodStopCommand   jsonrpc.MethodType = "command.stop"
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

const (
	KernelTypeShell      = "shell"      // Shell command type
	KernelTypeJavascript = "javascript" // JavaScript command type
)

type RunCommandRequest struct {
	SessionId uuid.UUID `json:"session_id"` // Unique identifier for the session.
	Kernel    string    `json:"kernel"`     // Type of command
	Data      string    `json:"command"`    // Command to be executed.
}

const (
	StreamStdout = "stdout" // Standard output stream
	StreamStderr = "stderr" // Standard error stream
	StreamClose  = "close"  // Stream close event
)

type CommandDataNotification struct {
	SessionId uuid.UUID `json:"session_id"`     // Unique identifier for the session.
	CommandId string    `json:"command_id"`     // Unique identifier for the command.
	Stream    string    `json:"stream"`         // Stream type (e.g., "stdout", "stderr").
	Data      string    `json:"data,omitempty"` // Data to be sent to the PTY, can be any JSON-serializable data.
}

type StopCommandRequest struct {
	SessionId uuid.UUID `json:"session_id"` // Unique identifier for the session.
	CommandId string    `json:"command_id"` // Unique identifier for the command.
}

type StopCommandResponse struct {
	SessionId uuid.UUID `json:"session_id"` // Unique identifier for the session.
	CommandId string    `json:"command_id"` // Unique identifier for the command.
}
