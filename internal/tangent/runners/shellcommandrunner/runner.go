// Package shellcommandrunner provides a runner for executing shell commands and scripts.
// It supports multiple runtime environments and configurable security settings.
// The package requires valid io.Writer implementations for output handling.
package shellcommandrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/tangent/runners"
)

// runner implements the runners.Runner interface.
// It manages command execution lifecycle and output streaming.
type runner struct {
	sessionID string
	config    Config
	writers   *IOWriters
}

// New creates a new runner with the given configuration.
// The configuration must be valid JSON that can be unmarshaled into a Config.
// The writers must provide non-nil io.Writer implementations for both stdout and stderr.
// Returns an error if the configuration is invalid or writers are not properly configured.
func New(ctx context.Context, sessionID string, jsonConfig json.RawMessage, writers *IOWriters) (runners.Runner, apperrors.Error) {
	var config Config

	if writers == nil || writers.Out == nil || writers.Err == nil {
		return nil, ErrInvalidWriters
	}

	if err := json.Unmarshal(jsonConfig, &config); err != nil {
		return nil, ErrInvalidConfig
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	runner := &runner{
		sessionID: sessionID,
		config:    config,
		writers:   writers,
	}

	return runner, nil
}

// Run executes the configured command.
// The context can be used to cancel the execution.
// Returns an error if execution fails or is cancelled.
func (r *runner) Run(ctx context.Context, args json.RawMessage) apperrors.Error {
	if r.config.Security.Type == SecurityTypeDevMode {
		return r.runWithDefaultSecurity(ctx, args)
	}
	return ErrInvalidSecurity.Msg("security type not supported: " + string(r.config.Security.Type))
}

func (r *runner) runWithDefaultSecurity(ctx context.Context, args json.RawMessage) apperrors.Error {
	scriptPath := filepath.Join(runnerConfig.ScriptDir, filepath.Clean(r.config.Script))
	if !strings.HasPrefix(scriptPath, filepath.Clean(runnerConfig.ScriptDir)+string(os.PathSeparator)) {
		return ErrInvalidScript.Msg("script path escapes trusted directory")
	}

	if _, err := os.Stat(scriptPath); err != nil {
		return ErrInvalidScript.Msg("script not found: " + err.Error())
	}

	homeDirPath := filepath.Join(os.TempDir(), r.sessionID)
	if err := os.MkdirAll(homeDirPath, 0755); err != nil {
		return ErrExecutionFailed.Msg("failed to create home directory: " + err.Error())
	}

	wrappedScriptPath := filepath.Join(homeDirPath, "wrapped.sh")
	if err := r.writeWrappedScript(wrappedScriptPath, scriptPath, args); err != nil {
		return ErrExecutionFailed.Msg("failed to create wrapped script: " + err.Error())
	}
	if err := os.Chmod(wrappedScriptPath, 0755); err != nil {
		return ErrExecutionFailed.Msg("failed to set permissions on wrapped script: " + err.Error())
	}

	baseEnv := os.Environ()
	env := appendOrReplaceEnv(baseEnv, "HOME", homeDirPath)
	for k, v := range r.config.Env {
		env = appendOrReplaceEnv(env, k, v)
	}

	cmd := exec.CommandContext(ctx, "/bin/bash", wrappedScriptPath)
	cmd.Dir = homeDirPath
	cmd.Env = env
	cmd.Stdout = r.writers.Out
	cmd.Stderr = r.writers.Err

	if err := cmd.Run(); err != nil {
		return ErrExecutionFailed.Msg("command failed: " + err.Error())
	}

	return nil
}

func (r *runner) writeWrappedScript(wrappedPath, scriptPath string, args json.RawMessage) error {
	var jsonObj any
	if err := json.Unmarshal(args, &jsonObj); err != nil {
		return fmt.Errorf("invalid JSON args: %w", err)
	}
	normalizedArgs, err := json.Marshal(jsonObj)
	if err != nil {
		return fmt.Errorf("could not normalize JSON args: %w", err)
	}
	escapedArgs := strings.ReplaceAll(string(normalizedArgs), "'", "'\\''")
	content := fmt.Sprintf(`#!/bin/bash
set -euo pipefail

exec '%s' '%s'
`, scriptPath, escapedArgs)

	return os.WriteFile(wrappedPath, []byte(content), 0644)
}

func appendOrReplaceEnv(env []string, key, value string) []string {
	prefix := key + "="
	for i, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}
