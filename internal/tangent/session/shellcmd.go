package session

import (
	"context"
	"io"
	"os"
	"os/exec"
	"syscall"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type shellConfig struct {
	dir string
}

// Executes a shell command, and dumps to stream
// This implements only home directory isolation and is only for use in a single user,
// fully trusted environment such as a local development environment.
func shellCmd(ctx context.Context, script string, c *shellConfig, io ...*commandIOWriters) apperrors.Error {
	if len(io) == 0 {
		io = append(io, &commandIOWriters{
			out: os.Stdout,
			err: os.Stderr,
		})
	}
	cmd, err := createCommand(ctx, script, io[0], c)
	if err != nil {
		return err
	}
	if err := cmd.Run(); err != nil {
		return ErrExecutionFailed.Msg(err.Error())
	}

	return nil
}

type commandIOWriters struct {
	out io.Writer
	err io.Writer
}

func createCommand(ctx context.Context, script string, s *commandIOWriters, c *shellConfig) (*exec.Cmd, apperrors.Error) {
	if c == nil {
		return nil, ErrSessionError.Msg("shell context is not set")
	}

	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return nil, ErrExecutionFailed.Msg("failed to create working directory: " + err.Error())
	}

	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", script)
	cmd.Dir = c.dir

	baseEnv := os.Environ()
	env := appendOrReplaceEnv(baseEnv, "HOME", c.dir)
	cmd.Env = env

	cmd.Stdout = s.out
	cmd.Stderr = s.err

	return cmd, nil
}

type commandErrorInfo struct {
	ExitCode int
	Signal   string
	Stderr   string
}

func extractCommandError(err error, stderr string) commandErrorInfo {
	info := commandErrorInfo{
		ExitCode: -1,
		Signal:   "",
		Stderr:   stderr,
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			info.ExitCode = status.ExitStatus()

			if status.Signaled() {
				info.Signal = status.Signal().String()
			}
		}
	}

	return info
}

func appendOrReplaceEnv(env []string, key, value string) []string {
	prefix := key + "="
	for i, kv := range env {
		if len(kv) >= len(prefix) && kv[:len(prefix)] == prefix {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}
