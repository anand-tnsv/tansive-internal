package session

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	srvsession "github.com/tansive/tansive-internal/internal/catalogsrv/session"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
)

func createSession(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest("request body is required")
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	req := &tangentcommon.SessionCreateRequest{}
	if err := json.Unmarshal(body, req); err != nil {
		return nil, httpx.ErrInvalidRequest("failed to parse request body: " + err.Error())
	}

	if !req.Interactive {
		return nil, httpx.ErrInvalidRequest("only interactive sessions are supported")
	}

	session, err := handleInteractiveSession(ctx, req)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-ndjson",
		Chunked:     true,
		WriteChunks: func(w http.ResponseWriter) error {
			return runSession(ctx, w, session)
		},
	}

	return rsp, nil
}

func handleInteractiveSession(ctx context.Context, req *tangentcommon.SessionCreateRequest) (*session, apperrors.Error) {
	client := getTansiveSrvClient()

	opts := httpclient.RequestOptions{
		Method: http.MethodPost,
		Path:   "sessions/execution-state",
		QueryParams: map[string]string{
			"code":          req.Code,
			"code_verifier": req.CodeVerifier,
		},
	}

	body, _, err := client.DoRequest(opts)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to create execution state")
		return nil, ErrFailedRequestToTansiveServer.Msg("unable to create execution state: " + err.Error())
	}

	rsp := &srvsession.SessionTokenRsp{}
	if err := json.Unmarshal(body, rsp); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to parse token response")
		return nil, ErrFailedRequestToTansiveServer.Msg("unable to parse token response: " + err.Error())
	}

	executionState, apperr := getExecutionState(ctx, rsp)
	if apperr != nil {
		return nil, apperr
	}

	ctx = log.Ctx(ctx).With().Str("session_id", executionState.SessionID.String()).Logger().WithContext(ctx)
	session, apperr := createActiveSession(ctx, executionState, rsp.Token, rsp.Expiry)
	if apperr != nil {
		log.Ctx(ctx).Error().Err(apperr).Msg("unable to create active session")
		return nil, apperr
	}
	return session, nil
}

func getExecutionState(ctx context.Context, rsp *srvsession.SessionTokenRsp) (*srvsession.ExecutionState, apperrors.Error) {
	if rsp.Token == "" {
		return nil, ErrTokenRequired
	}
	if rsp.Expiry.Before(time.Now()) {
		return nil, ErrTokenExpired
	}

	client := getHTTPClient(&clientConfig{
		token:       rsp.Token,
		tokenExpiry: rsp.Expiry,
		serverURL:   config.Config().TansiveServer.GetURL(),
	})

	opts := httpclient.RequestOptions{
		Method: http.MethodGet,
		Path:   "sessions/execution-state",
	}

	body, _, err := client.DoRequest(opts)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to get execution state")
		return nil, ErrFailedRequestToTansiveServer.Msg("unable to get execution state: " + err.Error())
	}

	executionState := &srvsession.ExecutionState{}
	if err := json.Unmarshal(body, executionState); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to parse execution state")
		return nil, ErrFailedRequestToTansiveServer.Msg("unable to parse execution state: " + err.Error())
	}

	log.Ctx(ctx).Info().Str("session_id", executionState.SessionID.String()).Msg("obtained execution state")

	return executionState, nil
}

func createActiveSession(ctx context.Context, executionState *srvsession.ExecutionState, token string, tokenExpiry time.Time) (*session, apperrors.Error) {
	serverCtx := &ServerContext{
		SessionID:        executionState.SessionID,
		SkillSet:         executionState.SkillSet,
		Skill:            executionState.Skill,
		View:             executionState.View,
		ViewDefinition:   executionState.ViewDefinition,
		SessionVariables: executionState.SessionVariables,
		InputArgs:        executionState.InputArgs,
		Catalog:          executionState.Catalog,
		Variant:          executionState.Variant,
		Namespace:        executionState.Namespace,
		TenantID:         executionState.TenantID,
	}

	session, err := ActiveSessionManager().CreateSession(ctx, serverCtx, token, tokenExpiry)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to create session")
		return nil, err
	}

	return session, nil
}

func runSession(ctx context.Context, w http.ResponseWriter, session *session) apperrors.Error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Ctx(ctx).Error().Msg("response writer does not support flushing")
		return ErrSessionError.Msg("response writer does not support flushing")
	}

	sessionLog, unsubSessionLog := GetEventBus().Subscribe(session.getTopic(TopicSessionLog), 100)
	defer unsubSessionLog()
	interactiveLog, unsubInteractiveLog := GetEventBus().Subscribe(session.getTopic(TopicInteractiveLog), 100)
	defer unsubInteractiveLog()

	logCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-sessionLog:
				data, ok := event.Data.([]byte)
				if !ok {
					continue
				}
				w.Write(data)
				flusher.Flush()
			case event := <-interactiveLog:
				data, ok := event.Data.([]byte)
				if !ok {
					continue
				}
				w.Write(data)
				flusher.Flush()
			}
		}
	}(logCtx)

	// Run will block until the session is complete
	log.Ctx(ctx).Info().Str("skill", session.context.Skill).Msg("running session")
	runCtx := session.getLogger(TopicSessionLog).With().Str("skill", session.context.Skill).Str("actor", "system").Logger().WithContext(ctx)
	apperr := session.Run(runCtx, "", session.context.Skill, session.context.InputArgs)
	cancel()
	wg.Wait()

	log.Ctx(ctx).Info().Msg("session completed")
	if apperr != nil {
		log.Ctx(ctx).Error().Err(apperr).Msg("session failed")
		return apperr
	}

	return nil
}
