package session

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/httpx"
)

func getSessionChannel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.SendError(w, ErrBadRequest)
		return
	}
	var sessionId uuid.UUID
	sessionId, err := uuid.Parse(id)
	if err != nil {
		httpx.SendError(w, ErrInvalidSession.Msg("invalid session ID"))
		return
	}
	session, apperr := ActiveSessionManager().GetSession(sessionId)
	if apperr != nil {
		httpx.SendError(w, apperr)
		return
	}

	// Check if the session has a valid connection
	if session.channel != nil && session.channel.conn != nil {
		httpx.SendError(w, ErrAlreadyExists.Msg("session already has an active channel"))
		return
	}

	// Upgrade the HTTP connection to a WebSocket connection
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // TODO: implement origin check
		},
		HandshakeTimeout: time.Second * 5, // max for internal use
		Subprotocols:     []string{"tansive.worker.v0.1"},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil || conn == nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to upgrade connection to WebSocket")
		httpx.SendError(w, ErrSessionError.Msg("failed to upgrade connection"))
		return
	}
	defer gracefulCloseWithCode(r.Context(), conn, websocket.CloseNormalClosure, "session channel closed")

	// Set the connection in the session
	session.channel = &channel{
		sessionId: sessionId,
		conn:      conn,
	}
	// pass this to the channel controller
	err = control(ctx, session.channel)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to control channel")
	}
}

func gracefulCloseWithCode(ctx context.Context, conn *websocket.Conn, code int, reason string) error {
	if conn == nil {
		return nil
	}

	closeMessage := websocket.FormatCloseMessage(code, reason)
	err := conn.WriteControl(
		websocket.CloseMessage,
		closeMessage,
		time.Now().Add(1*time.Second),
	)

	_ = conn.Close()
	log.Ctx(ctx).Info().Msgf("WebSocket connection closed with code %d: %s", code, reason)
	return err
}
