package session

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

func control(ctx context.Context, channel *channel) error {
	conn := channel.conn
	if conn == nil {
		return ErrChannelFailed.Msg("channel connection is nil")
	}
	return initializeChannel(ctx, channel.sessionId, channel)
}

func initializeChannel(ctx context.Context, sessionID uuid.UUID, channel *channel) apperrors.Error {
	initMsg := &InitChannelMessage{
		HeartbeatInterval: 30, // interval in seconds
	}
	conn := channel.conn
	if err := conn.WriteJSON(ChannelMessage{
		SessionId: sessionID,
		Type:      InitChannelMessageType,
		Data:      initMsg,
	}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to send initialization message")
		gracefulCloseWithCode(ctx, conn, websocket.CloseInternalServerErr, "channel initialization failed")
		return ErrChannelFailed.Msg("failed to initialize channel")
	}

	responseChan := make(chan ChannelMessageIn)
	errorChan := make(chan error)

	go func() {
		defer close(responseChan)
		defer close(errorChan)

		var resp ChannelMessageIn
		if err := conn.ReadJSON(&resp); err != nil {
			errorChan <- err
			return
		}
		responseChan <- resp
	}()

	select {
	case resp := <-responseChan:
		if resp.Type == InitChannelMessageType {
			log.Ctx(ctx).Info().Msg("Channel initialized successfully")
			if msg, err := getInitChannelMessage(resp.Data); err == nil && msg != nil {
				log.Ctx(ctx).Info().Msgf("Heartbeat interval set to %d seconds", msg.HeartbeatInterval)
				channel.peerHeartBeatInterval = time.Duration(msg.HeartbeatInterval) * time.Second
			} else {
				log.Ctx(ctx).Error().Msg("Invalid response data type for initialization")
				gracefulCloseWithCode(ctx, conn, ErrorUnexpectedMessageType, "invalid response data type for initialization")
				return ErrChannelFailed.Msg("invalid response data type for initialization")
			}
		} else {
			log.Ctx(ctx).Error().Msg("Unexpected message type received during initialization")
			gracefulCloseWithCode(ctx, conn, ErrorUnexpectedMessageType, "unexpected message type received during initialization")
			return ErrChannelFailed.Msg("unexpected message type received")
		}

	case err := <-errorChan:
		log.Ctx(ctx).Error().Err(err).Msg("Error reading response from channel")
		gracefulCloseWithCode(ctx, conn, websocket.CloseInternalServerErr, "error reading response from channel")
		return ErrChannelFailed.Msg("error reading response from channel")

	case <-time.After(10 * time.Second):
		log.Ctx(ctx).Error().Msg("Timeout waiting for channel initialization response")
		gracefulCloseWithCode(ctx, conn, ErrorTimeout, "timeout waiting for channel initialization response")
		return ErrChannelFailed.Msg("timeout waiting for channel initialization response")

	case <-ctx.Done():
		log.Ctx(ctx).Error().Msg("Context cancelled while waiting for channel initialization response")
		gracefulCloseWithCode(ctx, conn, websocket.CloseNormalClosure, "context cancelled while waiting for channel initialization response")
		return ErrChannelFailed.Msg("channel aborted")
	}

	return nil
}
