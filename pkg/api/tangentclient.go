package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type Client struct {
	httpClient *http.Client
	socketPath string
	config     clientConfig
}

type SkillInvocation struct {
	SessionID    string         `json:"session_id"`
	InvocationID string         `json:"invocation_id"`
	SkillName    string         `json:"skill_name"`
	Args         map[string]any `json:"args"`
}

type SkillResult struct {
	InvocationID string         `json:"invocation_id"`
	Output       map[string]any `json:"output"`
}

type ClientOption func(*clientConfig)

type clientConfig struct {
	dialTimeout time.Duration
	maxRetries  int
	retryDelay  time.Duration
}

func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.dialTimeout = timeout
	}
}

func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *clientConfig) {
		c.maxRetries = maxRetries
	}
}

func WithRetryDelay(delay time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.retryDelay = delay
	}
}

func NewClient(socketPath string, opts ...ClientOption) (*Client, error) {
	config := clientConfig{
		dialTimeout: 5 * time.Second,
		maxRetries:  3,
		retryDelay:  100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&config)
	}

	if socketPath == "" {
		return nil, fmt.Errorf("socket path is required")
	}

	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", socketPath)
		},
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   config.dialTimeout,
	}

	return &Client{
		httpClient: httpClient,
		socketPath: socketPath,
		config:     config,
	}, nil
}

func (c *Client) Close() error {
	// No persistent connection to close in net/http
	return nil
}

func (c *Client) InvokeSkill(ctx context.Context, sessionID, invocationID, skillName string, args map[string]any) (*SkillResult, error) {
	invocation := SkillInvocation{
		SessionID:    sessionID,
		InvocationID: invocationID,
		SkillName:    skillName,
		Args:         args,
	}

	body, err := json.Marshal(invocation)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal invocation: %w", err)
	}

	var lastErr error
	for i := 0; i < c.config.maxRetries; i++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "http://unix/skill-invocations", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(c.config.retryDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("skill invocation failed: %s", string(respBody))
		}

		var result SkillResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode result: %w", err)
		}
		return &result, nil
	}

	return nil, fmt.Errorf("failed to invoke skill after %d retries: %w", c.config.maxRetries, lastErr)
}

func (c *Client) GetTools(ctx context.Context, sessionID string) ([]LLMTool, error) {
	var lastErr error
	for i := 0; i < c.config.maxRetries; i++ {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://unix/tools?session_id=%s", sessionID), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(c.config.retryDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("get tools failed: %s", string(respBody))
		}

		var tools []LLMTool
		if err := json.NewDecoder(resp.Body).Decode(&tools); err != nil {
			return nil, fmt.Errorf("failed to decode tools: %w", err)
		}
		return tools, nil
	}

	return nil, fmt.Errorf("failed to get tools after %d retries: %w", c.config.maxRetries, lastErr)
}

func (c *Client) GetContext(ctx context.Context, sessionID, invocationID, name string) (any, error) {
	var lastErr error
	for i := 0; i < c.config.maxRetries; i++ {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://unix/context?session_id=%s&invocation_id=%s&name=%s", sessionID, invocationID, name), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(c.config.retryDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("get context failed: %s", string(respBody))
		}

		var context any
		if err := json.NewDecoder(resp.Body).Decode(&context); err != nil {
			return nil, fmt.Errorf("failed to decode context: %w", err)
		}
		return context, nil
	}

	return nil, fmt.Errorf("failed to get context after %d retries: %w", c.config.maxRetries, lastErr)
}

const DefaultSocketName = "tangent.service"

func GetSocketPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}
	runtimeDir := filepath.Join(homeDir, ".local", "run")
	if err := os.MkdirAll(runtimeDir, 0700); err != nil {
		return "", fmt.Errorf("failed to create runtime directory: %w", err)
	}
	return filepath.Join(runtimeDir, DefaultSocketName), nil
}
