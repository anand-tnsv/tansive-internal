package session

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/jsonrpc"
)

func (channel *channel) start(ctx context.Context) apperrors.Error {
	conn := channel.conn
	if conn == nil {
		return ErrChannelFailed.Msg("channel connection is nil")
	}
	if err := channel.initialize(ctx); err != nil {
		return err
	}

	// We generally call closeConn pretty liberally, so we use a sync.Once to ensure that we only close the connection once.
	ctxWithCancel, cancel := context.WithCancel(ctx)
	var once sync.Once
	closeConn := func(code int, reason string) {
		once.Do(func() {
			cancel()
			gracefulCloseWithCode(ctx, conn, code, reason)
		})
	}
	defer func() {
		closeConn(websocket.CloseNormalClosure, "channel control finished")
	}()

	chanReadFromPeer := make(chan []byte, 50)
	defer func() {
		close(chanReadFromPeer)
	}()

	var wg = &sync.WaitGroup{}

	// This goroutine reads messages from the peer.  The problem with Gorilla Websocket
	// is that it does not take context. Therefore, we need to close the connection to
	// signal this goroutine to bail out when necessary.
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Ctx(ctx).Error().Msgf("Recovered from panic: %v", r)
				closeConn(websocket.CloseInternalServerErr, "channel read i/o error")
			}
		}()
		for {
			if t, msg, err := conn.ReadMessage(); err != nil && t != websocket.TextMessage {
				log.Ctx(ctx).Error().Err(err).Msg("failed to read message from channel")
				closeConn(websocket.CloseInternalServerErr, "failed to read message from channel")
				return
			} else {
				chanReadFromPeer <- msg
			}
		}
	}(ctxWithCancel)

	sendToPeer := make(chan []byte, 50)
	defer func() {
		close(sendToPeer)
	}()

	// This goroutine sends messages to the peer. Here we primarily wait on the sendToPeer channel.
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Ctx(ctx).Error().Msgf("Recovered from panic: %v", r)
				closeConn(websocket.CloseInternalServerErr, "channel write i/o error")
			}
		}()
		for {
			select {
			case <-ctx.Done():
				log.Ctx(ctx).Info().Msg("Context cancelled, stopping send routine")
				closeConn(websocket.CloseNormalClosure, "context cancelled")
				return
			case msg := <-sendToPeer:
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					log.Ctx(ctx).Error().Err(err).Msg("failed to send message to channel")
					closeConn(websocket.CloseInternalServerErr, "failed to send message to channel")
					return
				}
			}
		}
	}(ctxWithCancel)

	// Set the channel's sendToPeer channel
	channel.reader = NewChannelMessageReader(chanReadFromPeer)
	channel.writer = NewChannelMessageWriter(sendToPeer)

	// Start sending heartbeats to the peer
	wg.Add(1)
	channel.sendHeartBeats(ctxWithCancel, wg)

	var apperror apperrors.Error = nil
recvLoop:
	for {
		select {
		case req := <-chanReadFromPeer:
			{
				msg, err := jsonrpc.ParseRequest(req)
				if err != nil {
					log.Ctx(ctx).Error().Err(err).Msg("failed to parse message from channel")
					closeConn(websocket.CloseInternalServerErr, "failed to parse message from channel")
					apperror = ErrChannelFailed.Msg("failed to parse message from channel")
					break recvLoop
				}
				if err := channel.handleRequest(ctx, msg); err != nil {
					if errors.Is(err, ErrBadRequest) {
						log.Ctx(ctx).Error().Msgf("Bad request received: %s", msg.Method)
						// this is probably indicative of dysfunctional state, so we close the connection
						closeConn(websocket.CloseInvalidFramePayloadData, "bad request")
						break recvLoop
					} else {
						log.Ctx(ctx).Error().Err(err).Msgf("Failed to handle request: %s", msg.Method)
					}
				}
			}
		case <-ctx.Done():
			log.Ctx(ctx).Info().Msg("Context cancelled, closing channel control")
			closeConn(websocket.CloseNormalClosure, "context cancelled")
			break recvLoop
		case <-ctxWithCancel.Done():
			log.Ctx(ctx).Info().Msg("Context with cancel done, closing channel control")
			closeConn(websocket.CloseNormalClosure, "context cancelled")
			break recvLoop

		case <-time.After(60 * time.Second): // Timeout for receiving messages
			log.Ctx(ctx).Warn().Msg("No messages received for 60 seconds, closing channel control")
			closeConn(websocket.CloseGoingAway, "receive timeout")
			apperror = ErrChannelFailed.Msg("receive timeout")
			break recvLoop
		}
	}
	// Wait for all goroutines to finish
	wg.Wait()
	return apperror
}

func (channel *channel) initialize(ctx context.Context) apperrors.Error {
	initMsg := &InitChannelNotification{
		SessionId:         channel.sessionId,
		HeartbeatInterval: 30, // interval in seconds
	}
	conn := channel.conn
	var once sync.Once
	closeConn := func(code int, reason string) {
		once.Do(func() {
			gracefulCloseWithCode(ctx, conn, code, reason)
		})
	}
	req, _ := jsonrpc.ConstructNotification(
		MethodInitChannel,
		initMsg,
	)

	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to send initialization message")
		closeConn(websocket.CloseInternalServerErr, "channel initialization failed")
		return ErrChannelFailed.Msg("failed to initialize channel")
	}

	responseChan := make(chan jsonrpc.Request, 1)
	errorChan := make(chan error, 1)
	defer func() {
		close(responseChan)
		close(errorChan)
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				closeConn(websocket.CloseInternalServerErr, "channel i/o error")
			}
			wg.Done()
		}()

		var resp jsonrpc.Request
		if err := conn.ReadJSON(&resp); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to read response from channel")
			errorChan <- err
			return
		}
		responseChan <- resp
	}()

	var apperror apperrors.Error
	select {
	case resp := <-responseChan:
		if resp.Method == MethodInitChannel {
			log.Ctx(ctx).Info().Msg("Channel initialized successfully")
			msg := InitChannelNotification{}
			if err := resp.Params.GetAs(&msg); err == nil {
				log.Ctx(ctx).Info().Msgf("Heartbeat interval set to %d seconds", msg.HeartbeatInterval)
				channel.peerHeartBeatInterval = time.Duration(msg.HeartbeatInterval) * time.Second
			} else {
				log.Ctx(ctx).Error().Msg("Invalid response data type for initialization")
				closeConn(ErrorUnexpectedMessageType, "not valid init message type")
				apperror = ErrChannelFailed.Msg("invalid response data type for initialization")
				break
			}
		} else {
			log.Ctx(ctx).Error().Msg("Unexpected message type received during initialization")
			closeConn(ErrorUnexpectedMessageType, "not valid init message type")
			apperror = ErrChannelFailed.Msg("unexpected message type received")
			break
		}

	case err := <-errorChan:
		log.Ctx(ctx).Error().Err(err).Msg("Error reading response from channel")
		closeConn(websocket.CloseInternalServerErr, "channel read error")
		apperror = ErrChannelFailed.Msg("error reading response from channel")
		break

	case <-time.After(10 * time.Second):
		log.Ctx(ctx).Error().Msg("Timeout waiting for channel initialization response")
		closeConn(ErrorTimeout, "channel initialization timeout")
		apperror = ErrChannelFailed.Msg("timeout waiting for channel initialization response")
		break

	case <-ctx.Done():
		log.Ctx(ctx).Error().Msg("Context cancelled while waiting for channel initialization response")
		closeConn(websocket.CloseNormalClosure, "complete")
		apperror = ErrChannelFailed.Msg("channel aborted")
		break
	}
	wg.Wait()
	return apperror
}

// sendHeartBeats sends periodic heartbeat messages to the channel.
// This is supposed to be a fire-and-forget routine that runs in the background.
// If the routine panics, the connection will eventually be closed due to the heartbeat timeout.
func (channel *channel) sendHeartBeats(ctx context.Context, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Ctx(ctx).Error().Msgf("Recovered from panic in heartbeat sender: %v", r)
			}
		}()
		ticker := time.NewTicker(channel.peerHeartBeatInterval)
		defer ticker.Stop()
		heartbeatMessage, _ := jsonrpc.ConstructNotification(
			MethodHeartbeat,
			&HeartbeatNotification{
				SessionId: channel.sessionId,
			},
		)
		for {
			select {
			case <-ticker.C:
				if channel.writer != nil {

					channel.writer.WriteMessage(heartbeatMessage)
				}
			case <-ctx.Done():
				log.Ctx(ctx).Info().Msg("Stopping heartbeat sender")
				return
			}
		}
	}()
}

func (channel *channel) handleRequest(ctx context.Context, req *jsonrpc.Request) apperrors.Error {
	if channel.reader == nil || channel.writer == nil {
		return ErrChannelFailed.Msg("channel not initialized")
	}
	switch req.Method {
	case MethodInitChannel:
		log.Ctx(ctx).Error().Msg("Received InitChannel request, but this should not happen after initialization")
		return ErrBadRequest.Msg("init channel request received after initialization")

	case MethodHeartbeat:
		log.Ctx(ctx).Info().Msg("Received Heartbeat request")
		return nil // Heartbeat does not require any action

	case MethodStartTerminal:
		log.Ctx(ctx).Info().Msg("Received StartPty request")
		if err := HandleStartTerminal(ctx, req, channel.writer, channel.ttys, channel.commandContext.shellConfig); err != nil {
			return err
		}

	case MethodStopTerminal:
		log.Ctx(ctx).Info().Msg("Received StopPty request")
		if err := HandleStopTerminal(ctx, req, channel.writer, channel.ttys); err != nil {
			return err
		}

	case MethodTerminalData:
		log.Ctx(ctx).Info().Msg("Received TerminalData request")
		if err := HandleTerminalData(ctx, req, channel.writer, channel.ttys); err != nil {
			return err
		}

	case MethodRunCommand:
		log.Ctx(ctx).Info().Msg("Received RunCommand request")
		if err := HandleRunCommandRequest(ctx, req, channel.writer, channel.commandContext); err != nil {
			return err
		}

	case MethodStopCommand:
		log.Ctx(ctx).Info().Msg("Received StopCommand request")
		if err := HandleStopCommandRequest(ctx, req, channel.writer, channel.commandContext); err != nil {
			return err
		}

	default:
		log.Ctx(ctx).Error().Msgf("Unknown method: %s", req.Method)
		return ErrUnknownMethod
	}

	return nil
}

func sendJsonRpcError(ctx context.Context, w MessageWriter, id string, code int, msg string, data any) apperrors.Error {
	rsp, err := jsonrpc.ConstructErrorResponse(id, code, msg, data)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to construct error response for invalid terminal ID")
		return nil
	}
	if err := w.WriteMessage(rsp); err != nil {
		return ErrChannelFailed.Msg("failed to write error response")
	}
	return nil
}
