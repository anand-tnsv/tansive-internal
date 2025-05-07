package jsonrpc

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tansive/tansive-internal/pkg/types"
)

// Helper functions to deal with JSON-RPC 2.0 requests and responses

// JSON-RPC version
const Version = "2.0"

type MethodType string

// Request represents a JSON-RPC 2.0 request or notification
type Request struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      string            `json:"id,omitempty"`
	Method  MethodType        `json:"method"`
	Params  types.NullableAny `json:"params,omitempty"`
}

// Response represents a JSON-RPC 2.0 response
type Response struct {
	JSONRPC string       `json:"jsonrpc"`
	ID      string       `json:"id"`
	Result  any          `json:"result,omitempty"`
	Error   *ErrorObject `json:"error,omitempty"`
}

// ErrorObject represents a JSON-RPC 2.0 error object
type ErrorObject struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// ConstructRequest creates a JSON-RPC request message
func ConstructRequest(id string, method MethodType, params any) ([]byte, error) {
	p, err := types.NullableAnyFrom(params)
	if err != nil {
		return nil, err
	}
	req := Request{
		JSONRPC: Version,
		ID:      id,
		Method:  method,
		Params:  p,
	}
	return json.Marshal(req)
}

// ConstructNotification creates a JSON-RPC notification (no response expected)
func ConstructNotification(method MethodType, params any) ([]byte, error) {
	p, err := types.NullableAnyFrom(params)
	if err != nil {
		return nil, err
	}
	req := Request{
		JSONRPC: Version,
		Method:  method,
		Params:  p,
	}
	return json.Marshal(req)
}

// ConstructSuccessResponse creates a JSON-RPC response with a result
func ConstructSuccessResponse(id string, result any) ([]byte, error) {
	resp := Response{
		JSONRPC: Version,
		ID:      id,
		Result:  result,
	}
	return json.Marshal(resp)
}

// ConstructErrorResponse creates a JSON-RPC error response
func ConstructErrorResponse(id string, code int, message string, data any) ([]byte, error) {
	resp := Response{
		JSONRPC: Version,
		ID:      id,
		Error: &ErrorObject{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
	return json.Marshal(resp)
}

// ParseRequest unmarshals a JSON-RPC request or notification
func ParseRequest(data []byte) (*Request, error) {
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	if req.JSONRPC != Version || req.Method == "" {
		return nil, errors.New("invalid JSON-RPC request")
	}
	return &req, nil
}

// ParseResponse unmarshals a JSON-RPC response
func ParseResponse(data []byte) (*Response, error) {
	var resp Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.JSONRPC != Version {
		return nil, errors.New("invalid JSON-RPC response")
	}
	if resp.Result != nil && resp.Error == nil {
		return nil, errors.New("response must have either result or error")
	}
	return &resp, nil
}

// Example error codes from JSON-RPC 2.0 spec
const (
	ErrCodeParseError        = -32700
	ErrCodeInvalidRequest    = -32600
	ErrCodeMethodNotFound    = -32601
	ErrCodeInvalidParams     = -32602
	ErrCodeInternalError     = -32603
	ErrCodeConcurrentCommand = -32001
	ErrCodeBadCommand        = -32002
)

// FormatErrorMessage returns a user-friendly error message from an error
func FormatErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("JSON-RPC error: %s", err.Error())
}
