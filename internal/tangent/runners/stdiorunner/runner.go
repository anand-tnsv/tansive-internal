// Package shellcommandrunner provides a runner for executing shell commands and scripts.
// It supports multiple runtime environments and configurable security settings.
// The package requires valid io.Writer implementations for output handling.
package stdiorunner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
	"github.com/tansive/tansive-internal/pkg/api"
)

// runner implements the runners.Runner interface.
// It manages command execution lifecycle and output streaming.
type runner struct {
	sessionID   string
	config      Config
	homeDirPath string
	writers     []*tangentcommon.IOWriters
}

func (r *runner) ID() string {
	return catcommon.StdioRunnerID
}

func (r *runner) AddWriters(writers ...*tangentcommon.IOWriters) {
	r.writers = append(r.writers, writers...)
}

// New creates a new runner with the given configuration.
// The configuration must be valid JSON that can be unmarshaled into a Config.
// The writers must provide non-nil io.Writer implementations for both stdout and stderr.
// Returns an error if the configuration is invalid or writers are not properly configured.
func New(ctx context.Context, sessionID string, configMap map[string]any, writers ...*tangentcommon.IOWriters) (*runner, apperrors.Error) {
	var config Config

	for _, writer := range writers {
		if writer == nil || writer.Out == nil || writer.Err == nil {
			return nil, ErrInvalidWriters
		}
	}

	if err := mapstructure.Decode(configMap, &config); err != nil {
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
// DevMode security allows execution of scripts from the configured script directory
// with minimal restrictions, intended for development and testing purposes only.
// NOT FOR PRODUCTION USE - lacks security measures required for production environments.
func (r *runner) Run(ctx context.Context, args *api.SkillInputArgs) apperrors.Error {
	if args == nil {
		return ErrInvalidArgs.Msg("args is nil")
	}

	if r.config.Security.Type == SecurityTypeDevMode {
		return r.runWithDevModeSecurity(ctx, args)
	}
	return ErrInvalidSecurity.Msg("security type not supported: " + string(r.config.Security.Type))
}

func (r *runner) runWithDevModeSecurity(ctx context.Context, args *api.SkillInputArgs) apperrors.Error {
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

	r.homeDirPath = homeDirPath
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

	outWriter := NewWriter(StdoutWriter, r.writers...)
	errWriter := NewWriter(StderrWriter, r.writers...)

	cmd := exec.CommandContext(ctx, "/bin/bash", wrappedScriptPath)
	cmd.Dir = homeDirPath
	cmd.Env = env
	cmd.Stdout = outWriter
	cmd.Stderr = errWriter

	if err := cmd.Run(); err != nil {
		return ErrExecutionFailed.Msg("command failed: " + err.Error())
	}

	return nil
}

func (r *runner) writeWrappedScript(wrappedPath, scriptPath string, args *api.SkillInputArgs) error {
	jsonArgs, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("could not normalize JSON args: %w", err)
	}

	escapedArgs := strings.ReplaceAll(string(jsonArgs), "'", "'\\''")

	var content string
	if r.config.Runtime == RuntimeBinary {
		content = fmt.Sprintf(`#!/bin/bash
set -euo pipefail

exec '%s' '%s'
`, scriptPath, escapedArgs)
	} else {
		execCmd, err := resolveRuntimeCommand(r.config.Runtime)
		if err != nil {
			return fmt.Errorf("unsupported runtime: %w", err)
		}

		content = fmt.Sprintf(`#!/bin/bash
set -euo pipefail

exec '%s' '%s' '%s'
`, execCmd, scriptPath, escapedArgs)
	}

	return os.WriteFile(wrappedPath, []byte(content), 0644)
}

func resolveRuntimeCommand(runtime Runtime) (string, error) {
	switch runtime {
	case RuntimeBash:
		return "/bin/bash", nil
	case RuntimePython:
		return "python3", nil
	case RuntimeNode:
		return "node", nil
	case RuntimeNPX:
		return "npx", nil
	case RuntimeNPM:
		return "npm", nil
	case RuntimeBinary:
		return "", nil
	default:
		return "", fmt.Errorf("invalid runtime: %s", runtime)
	}
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

func (r *runner) GetHomeDirPath() string {
	return r.homeDirPath
}
