package session

import (
	"context"
	"errors"
	"os/exec"
	"syscall"

	"github.com/creack/pty"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/jsonrpc"
)

func HandleStartTerminal(ctx context.Context, msg *jsonrpc.Request, w MessageWriter, ptys Ttys) apperrors.Error {
	req := &StartTerminalRequest{}
	if err := msg.Params.GetAs(req); err != nil {
		return ErrInvalidParams
	}
	// check if the pty already exists
	if _, exists := ptys[req.TerminalId]; exists {
		rsp, err := jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInvalidParams, "pty already exists", nil)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for existing pty")
			return nil
		}
		if err := w.WriteMessage(rsp); err != nil {
			return ErrChannelFailed.Msg("failed to write error response")
		}
		return nil
	}
	// Create a new pty and add it to the map
	if apperr := createPtySession(ctx, req, w, ptys); apperr != nil {
		var rsp []byte
		if errors.Is(apperr, ErrInvalidParams) {
			var err error
			rsp, err = jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInvalidParams, apperr.Error(), nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for invalid terminal ID")
				return nil
			}
		} else if errors.Is(apperr, ErrChannelFailed) {
			var err error
			rsp, err = jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInternalError, apperr.Error(), nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for failed pty start")
				return nil
			}
		} else {
			var err error
			rsp, err = jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInternalError, "failed to start pty", nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for failed pty start")
				return nil
			}
		}
		if err := w.WriteMessage(rsp); err != nil {
			return ErrChannelFailed.Msg("failed to write error response")
		}
	}
	return nil
}

func HandleStopTerminal(ctx context.Context, msg *jsonrpc.Request, w MessageWriter, ptys Ttys) apperrors.Error {
	req := &StopTerminalRequest{}
	if err := msg.Params.GetAs(req); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to parse stop terminal request parameters")
		rsp, err := jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInvalidParams, "invalid parameters", nil)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for invalid parameters")
			return nil
		}
		if err := w.WriteMessage(rsp); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to write error response for invalid parameters")
			return ErrChannelFailed.Msg("failed to write error response")
		}
		return nil
	}

	if apperr := stopPtySession(ctx, req, w, ptys); apperr != nil {
		var rsp []byte
		if errors.Is(apperr, ErrInvalidParams) {
			var err error
			rsp, err = jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInvalidParams, apperr.Error(), nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for invalid terminal ID")
				return nil
			}
		} else if errors.Is(apperr, ErrChannelFailed) {
			var err error
			rsp, err = jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInternalError, apperr.Error(), nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for failed pty start")
				return nil
			}
		} else {
			var err error
			rsp, err = jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInternalError, "failed to start pty", nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for failed pty start")
				return nil
			}
		}
		if err := w.WriteMessage(rsp); err != nil {
			return ErrChannelFailed.Msg("failed to write error response")
		}
	}
	log.Ctx(ctx).Info().Msgf("stopped pty session with ID: %s", req.TerminalId.String())
	return nil
}

func createPtySession(ctx context.Context, req *StartTerminalRequest, w MessageWriter, ptys Ttys) apperrors.Error {
	// Create a new pty session
	if req.TerminalId == uuid.Nil {
		log.Ctx(ctx).Error().Msg("invalid terminal ID format")
		return ErrInvalidParams.Msg("invalid terminal ID format")
	}

	doneNotification := func() {
		defer func() {
			if r := recover(); r != nil {
				log.Ctx(ctx).Error().Msgf("recovered from panic in done notification: %v", r)
			}
		}()
		cleanUpTty(ctx, req.TerminalId, ptys)
		msg, _ := jsonrpc.ConstructNotification(
			MethodTerminalData,
			TerminalDataNotification{
				SessionId:  req.SessionId,
				TerminalId: req.TerminalId,
				Marker:     DoneMarker(),
			},
		)
		if err := w.WriteMessage(msg); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to write done notification")
		}
	}

	cmd := exec.Command("zsh", "-li")
	ptmx, err := pty.StartWithAttrs(cmd, nil, &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
		Ctty:    0, // Will override below
	})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to start pty")
		return ErrChannelFailed.Msg("failed to start pty")
	}
	cmd.SysProcAttr.Ctty = int(ptmx.Fd())
	// Create a recorder
	recorder, err := NewAsciinemaWriter(req.SessionId.String() + "-" + req.TerminalId.String() + ".cast")
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create asciinema recorder")
	}

	// capture output
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Ctx(ctx).Error().Msgf("recovered from panic in pty output capture: %v", r)
			}
			doneNotification()
		}()

		buf := make([]byte, 1024)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				data := buf[:n]
				msg, _ := jsonrpc.ConstructNotification(
					MethodTerminalData,
					TerminalDataNotification{
						SessionId:  req.SessionId,
						TerminalId: req.TerminalId,
						Marker:     NewMarker(),
						Data:       string(data), // this creates a copy of the data, so the buffer can be reused
					},
				)
				w.WriteMessage(msg)
				if recorder != nil {
					recorder.Write("o", string(data))
				}
			}
			if err != nil {
				break
			}
		}
	}()

	ptys[req.TerminalId] = &tty{
		terminalId: req.TerminalId,
		ptmx:       ptmx,
		cmd:        cmd,
		recorder:   recorder,
	}
	log.Ctx(ctx).Info().Msgf("created new pty session with ID: %s", req.TerminalId.String())
	return nil
}

func stopPtySession(ctx context.Context, req *StopTerminalRequest, w MessageWriter, ptys Ttys) apperrors.Error {
	if !cleanUpTty(ctx, req.TerminalId, ptys) {
		return nil
	}
	msg, _ := jsonrpc.ConstructNotification(
		MethodTerminalData,
		TerminalDataNotification{
			SessionId:  req.SessionId,
			TerminalId: req.TerminalId,
			Marker:     DoneMarker(),
		},
	)
	w.WriteMessage(msg)

	delete(ptys, req.TerminalId)
	log.Ctx(ctx).Info().Msgf("stopped pty session with ID: %s", req.TerminalId.String())
	return nil
}

func cleanUpTty(ctx context.Context, terminalId uuid.UUID, ptys Ttys) bool {
	tty, exists := ptys[terminalId]
	if !exists {
		log.Ctx(ctx).Warn().Msgf("pty session with ID %s does not exist for cleanup", terminalId.String())
		return false
	}
	delete(ptys, terminalId)
	if tty.cmd != nil && tty.cmd.Process != nil {
		_ = tty.cmd.Process.Kill() // force terminate
	}
	if tty.ptmx != nil {
		_ = tty.ptmx.Close()
	}
	if tty.cmd != nil {
		_ = tty.cmd.Wait()
	}
	if tty.recorder != nil {
		tty.recorder.Close()
	}
	log.Ctx(ctx).Info().Msgf("cleaned up pty session with ID: %s", tty.terminalId.String())
	return true
}

func HandleTerminalData(ctx context.Context, msg *jsonrpc.Request, w MessageWriter, ptys Ttys) apperrors.Error {
	req := &TerminalDataNotification{}
	if err := msg.Params.GetAs(req); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to parse stop terminal request parameters")
		rsp, err := jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInvalidParams, "invalid parameters", nil)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for invalid parameters")
			return nil
		}
		if err := w.WriteMessage(rsp); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to write error response for invalid parameters")
			return ErrChannelFailed.Msg("failed to write error response")
		}
		return nil
	}

	tty, exists := ptys[req.TerminalId]
	if !exists {
		log.Ctx(ctx).Error().Msgf("pty session with ID %s does not exist", req.TerminalId.String())
		rsp, err := jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInvalidParams, "invalid parameters", nil)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for invalid parameters")
			return nil
		}
		if err := w.WriteMessage(rsp); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to write error response for invalid parameters")
			return ErrChannelFailed.Msg("failed to write error response")
		}
		return nil
	}

	if req.Data != "" && tty.ptmx != nil {
		// Write the data to the pty
		if _, err := tty.ptmx.Write([]byte(req.Data)); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to write data to pty")
			rsp, err := jsonrpc.ConstructErrorResponse(msg.ID, jsonrpc.ErrCodeInternalError, "failed to write to pty", nil)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for failed write")
				return nil
			}
			if err := w.WriteMessage(rsp); err != nil {
				return ErrChannelFailed.Msg("failed to write error response")
			}
			return nil
		}
		if tty.recorder != nil {
			tty.recorder.Write("i", string(req.Data))
		}
		log.Ctx(ctx).Info().Msgf("wrote data to pty session with ID: %s", req.TerminalId.String())
	}

	return nil
}
