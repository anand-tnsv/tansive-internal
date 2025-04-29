package session

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Message Error Codes
const (
	ErrorUnexpectedMessageType = 4409 // Unexpected message type received
	ErrorTimeout               = 4504 // Timeout error, e.g., for heartbeat or response waiting
)

type ChannelMessage struct {
	// SessionId is the ID of the session this message belongs to.
	SessionId uuid.UUID          `json:"session_id"`
	Type      ChannelMessageType `json:"type"`
	Data      any                `json:"data,omitempty"` // Data can be any type, depending on the message type.
}

type ChannelMessageType string

const (
	InitChannelMessageType ChannelMessageType = "init"
	HeartbeatMessageType   ChannelMessageType = "heartbeat"
)

type InitChannelMessage struct {
	HeartbeatInterval int64 `json:"heartbeat_interval"` // Heartbeat interval in seconds.
}

func getInitChannelMessage(data json.RawMessage) (*InitChannelMessage, error) {
	var msg InitChannelMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

type ChannelMessageIn struct {
	SessionId uuid.UUID          `json:"session_id"`
	Type      ChannelMessageType `json:"type"`
	Data      json.RawMessage    `json:"data,omitempty"` // Data can be any type, depending on the message type.
}
