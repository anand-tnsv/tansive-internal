package session

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/jsonrpc"
)

func HandleRunCommandRequest(ctx context.Context, msg *jsonrpc.Request, w MessageWriter, c *commandContext) apperrors.Error {
	req := &RunCommandRequest{}
	if err := msg.Params.GetAs(req); err != nil {
		return sendJsonRpcError(ctx, w, msg.ID, jsonrpc.ErrCodeInvalidParams, "invalid parameters", nil)
	}
	// try to obtain mutex
	if !c.mu.TryLock() {
		log.Ctx(ctx).Error().Msg("failed to obtain command context lock")
		return sendJsonRpcError(ctx, w, msg.ID, jsonrpc.ErrCodeConcurrentCommand, "another command already running", nil)
	}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			c.mu.Unlock()
			if c.cancel != nil {
				c.cancel()
				c.cancel = nil
			}
			c.requestId = ""
		})
	}
	if req.Kernel == KernelTypeShell {
		//TODO: run preprocessor
		cmdCtx, cancel := context.WithCancel(ctx)
		c.cancel = func() {
			cancel()
		}
		go func() {
			defer cleanup()
			defer func() {
				if r := recover(); r != nil {
					log.Ctx(cmdCtx).Error().Msgf("command execution panicked: %v", r)
					sendJsonRpcError(cmdCtx, w, msg.ID, jsonrpc.ErrCodeInternalError, "command execution could not complete", nil)
				}
			}()
			err := shellCmd(cmdCtx, req.Data, c.shellConfig, getCommandIOWriters(cmdCtx, w, msg.ID, req.SessionId, func() {
				log.Ctx(cmdCtx).Error().Msg("spurious escape sequence detected, killing command")
				sendJsonRpcError(cmdCtx, w, msg.ID, jsonrpc.ErrCodeBadCommand, "command terminated: interactive or full-screen commands like 'vi' are not supported", nil)
				cancel()
			}))
			if err != nil {
				log.Ctx(cmdCtx).Error().Err(err).Msg("failed to run shell command")
				sendJsonRpcError(cmdCtx, w, msg.ID, jsonrpc.ErrCodeInternalError, err.Error(), nil)
			}
			msg, apperr := jsonrpc.ConstructNotification(
				MethodCommandData,
				&CommandDataNotification{
					SessionId: req.SessionId,
					CommandId: msg.ID,
					Stream:    StreamClose,
					Data:      "",
				},
			)
			if apperr != nil {
				log.Ctx(cmdCtx).Error().Err(apperr).Msg("failed to construct command response")
				return
			}
			if err := w.WriteMessage(msg); err != nil {
				log.Ctx(cmdCtx).Error().Err(err).Msg("failed to write command response")
				return
			}
			log.Ctx(cmdCtx).Debug().Msg("command execution completed")
		}()
		c.requestId = msg.ID
	} else if req.Kernel == KernelTypeJavascript {
		// do nothing for now
	}
	return nil
}

func HandleStopCommandRequest(ctx context.Context, msg *jsonrpc.Request, w MessageWriter, c *commandContext) apperrors.Error {
	req := &StopCommandRequest{}
	if err := msg.Params.GetAs(req); err != nil {
		return sendJsonRpcError(ctx, w, msg.ID, jsonrpc.ErrCodeInvalidParams, "invalid parameters", nil)
	}
	if c.requestId == "" {
		return sendJsonRpcError(ctx, w, msg.ID, jsonrpc.ErrCodeInvalidParams, "no command running", nil)
	}
	if c.cancel != nil {
		log.Ctx(ctx).Debug().Msg("stopping command")
		c.cancel()
		c.cancel = nil
	}
	c.requestId = ""
	return nil
}

type writerFunc func(p []byte) (n int, err error)

func (f writerFunc) Write(p []byte) (n int, err error) {
	n, err = f(p)
	return
}

func getCommandIOWriters(ctx context.Context, w MessageWriter, msgId string, sessionId uuid.UUID, kill func()) *commandIOWriters {
	f := func(stream string) writerFunc {
		return writerFunc(func(p []byte) (n int, err error) {
			if stream == StreamStderr || stream == StreamStdout {
				if containsSuspiciousEscape(p) {
					log.Ctx(ctx).Warn().Msg("escape sequence detected, killing command")
					kill()
					return 0, errors.New("full screen or interactive commands are not supported")
				}
			}

			msg, err := jsonrpc.ConstructNotification(
				MethodCommandData,
				&CommandDataNotification{
					SessionId: sessionId,
					CommandId: msgId,
					Stream:    stream,
					Data:      string(p),
				},
			)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct command response")
				return 0, err
			}
			if err := w.WriteMessage(msg); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to write command response")
				return 0, err
			}
			return len(p), nil
		})
	}
	return &commandIOWriters{
		out: f(StreamStdout),
		err: f(StreamStderr),
	}
}

func containsSuspiciousEscape(p []byte) bool {
	sequences := [][]byte{
		[]byte("\x1b[?1049h"), // alternate screen
		[]byte("\x1b[2J"),     // clear screen
		[]byte("\x1b[?25l"),   // hide cursor
	}
	for _, seq := range sequences {
		if bytes.Contains(p, seq) {
			return true
		}
	}
	return false
}
