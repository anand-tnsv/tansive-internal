package shellcommandrunner

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

func init() {
	// Override the default script directory with an absolute path
	runnerConfig.ScriptDir = filepath.Join(os.Getenv("HOME"), "tansive_scripts")
}

func TestNew(t *testing.T) {
	tests := []struct {
		name       string
		jsonConfig json.RawMessage
		wantErr    bool
		errorType  apperrors.Error
		check      func(t *testing.T, r *runner)
	}{
		{
			name: "valid config",
			jsonConfig: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {
					"key1": "value1"
				},
				"env": {
					"VAR1": "value1"
				},
				"script": "test.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			wantErr: false,
		},
		{
			name: "environment variables set correctly",
			jsonConfig: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value",
					"FOO": "bar",
					"BAZ": "qux"
				},
				"script": "test.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			wantErr: false,
			check: func(t *testing.T, r *runner) {
				assert.Equal(t, "test_value", r.config.Env["TEST_VAR"])
				assert.Equal(t, "bar", r.config.Env["FOO"])
				assert.Equal(t, "qux", r.config.Env["BAZ"])
			},
		},
		{
			name:       "invalid json",
			jsonConfig: json.RawMessage(`{invalid json}`),
			wantErr:    true,
			errorType:  ErrInvalidConfig,
		},
		{
			name: "invalid runtime",
			jsonConfig: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "invalid",
				"runtimeConfig": {},
				"env": {},
				"script": "test.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			wantErr:   true,
			errorType: ErrInvalidRuntime,
		},
		{
			name: "invalid security type",
			jsonConfig: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {},
				"script": "test.sh",
				"security": {
					"type": "invalid"
				}
			}`),
			wantErr:   true,
			errorType: ErrInvalidSecurity,
		},
		{
			name: "missing script",
			jsonConfig: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {},
				"security": {
					"type": "dev-mode"
				}
			}`),
			wantErr:   true,
			errorType: ErrInvalidScript,
		},
		{
			name: "incompatible version",
			jsonConfig: json.RawMessage(`{
				"version": "1.0.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {},
				"script": "test.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			wantErr:   true,
			errorType: ErrInvalidVersion,
		},
		{
			name: "invalid version format",
			jsonConfig: json.RawMessage(`{
				"version": "invalid-version",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {},
				"script": "test.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			wantErr:   true,
			errorType: ErrInvalidVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			writers := &IOWriters{
				Out: io.Discard,
				Err: io.Discard,
			}

			r, err := New(ctx, "test-session", tt.jsonConfig, writers)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errorType != nil {
					assert.Equal(t, tt.errorType, err)
				}
				assert.Nil(t, r)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, r)
				if tt.check != nil {
					tt.check(t, r.(*runner))
				}
			}
		})
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name      string
		config    json.RawMessage
		args      json.RawMessage
		wantErr   bool
		errorType apperrors.Error
		check     func(t *testing.T, stdout, stderr string)
	}{
		{
			name: "successful execution",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "test_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1",
				"arg2": "value2"
			}`),
			wantErr: false,
		},
		{
			name: "environment variables passed correctly",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value",
					"FOO": "bar",
					"BAZ": "qux"
				},
				"script": "test_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1",
				"arg2": "value2",
				"check_env": true
			}`),
			wantErr: false,
			check: func(t *testing.T, stdout, stderr string) {
				assert.Contains(t, stdout, "TEST_VAR=test_value")
				assert.Contains(t, stdout, "FOO=bar")
				assert.Contains(t, stdout, "BAZ=qux")

			},
		},
		{
			name: "script not found",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "non_existent_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1",
				"arg2": "value2"
			}`),
			wantErr:   true,
			errorType: ErrInvalidScript,
		},
		{
			name: "invalid JSON arguments",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "test_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args:      json.RawMessage(`{invalid json}`),
			wantErr:   true,
			errorType: ErrExecutionFailed,
		},
		{
			name: "script execution failure",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "test_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1",
				"arg2": "value2",
				"should_fail": true
			}`),
			wantErr:   true,
			errorType: ErrExecutionFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var stdout, stderr strings.Builder
			writers := &IOWriters{
				Out: &stdout,
				Err: &stderr,
			}

			runner, err := New(ctx, "test-session", tt.config, writers)
			require.NoError(t, err)
			require.NotNil(t, runner)

			err = runner.Run(ctx, tt.args)
			t.Logf("stdout: %s", stdout.String())
			t.Logf("stderr: %s", stderr.String())
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errorType != nil {
					assert.Equal(t, tt.errorType, err)
				}
			} else {
				assert.NoError(t, err)
				if tt.check != nil {
					tt.check(t, stdout.String(), stderr.String())
				}
			}
		})
	}
}

func TestDevModeSecurity(t *testing.T) {
	tests := []struct {
		name      string
		config    json.RawMessage
		args      json.RawMessage
		wantErr   bool
		errorType apperrors.Error
		check     func(t *testing.T, stdout, stderr string)
	}{
		{
			name: "script path escaping attempt",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "../../../etc/passwd",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1",
				"arg2": "value2"
			}`),
			wantErr:   true,
			errorType: ErrInvalidScript,
		},
		{
			name: "command injection attempt",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "test_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1; rm -rf /",
				"arg2": "value2; cat /etc/passwd"
			}`),
			wantErr: false,
			check: func(t *testing.T, stdout, stderr string) {
				assert.Contains(t, stdout, "arg1: value1; rm -rf /")
				assert.Contains(t, stdout, "arg2: value2; cat /etc/passwd")
			},
		},
		{
			name: "home directory access attempt",
			config: json.RawMessage(`{
				"version": "0.1.0",
				"runtime": "bash",
				"runtimeConfig": {},
				"env": {
					"TEST_VAR": "test_value"
				},
				"script": "test_script.sh",
				"security": {
					"type": "dev-mode"
				}
			}`),
			args: json.RawMessage(`{
				"arg1": "value1",
				"arg2": "value2",
				"check_home": true
			}`),
			wantErr: false,
			check: func(t *testing.T, stdout, stderr string) {
				assert.Contains(t, stdout, "Home directory contents:")
				assert.Contains(t, stdout, "total")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var stdout, stderr strings.Builder
			writers := &IOWriters{
				Out: &stdout,
				Err: &stderr,
			}

			runner, err := New(ctx, "test-session", tt.config, writers)
			require.NoError(t, err)
			require.NotNil(t, runner)

			err = runner.Run(ctx, tt.args)
			t.Logf("stdout: %s", stdout.String())
			t.Logf("stderr: %s", stderr.String())
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errorType != nil {
					assert.Equal(t, tt.errorType, err)
				}
			} else {
				assert.NoError(t, err)
				if tt.check != nil {
					tt.check(t, stdout.String(), stderr.String())
				}
			}
		})
	}
}
