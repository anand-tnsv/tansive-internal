package session

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	srvsession "github.com/tansive/tansive-internal/internal/catalogsrv/session"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/common/httpx"
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

	err = handleInteractiveSession(ctx, req)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode:  http.StatusOK,
		Response:    "ok",
		ContentType: "text/plain",
	}
	return rsp, nil
}

func handleInteractiveSession(ctx context.Context, req *tangentcommon.SessionCreateRequest) apperrors.Error {
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
		return ErrFailedRequestToTansiveServer.Msg("unable to create execution state: " + err.Error())
	}

	rsp := &srvsession.SessionTokenRsp{}
	if err := json.Unmarshal(body, rsp); err != nil {
		return ErrFailedRequestToTansiveServer.Msg("unable to parse token response: " + err.Error())
	}

	executionState, apperr := getExecutionState(ctx, rsp)
	if apperr != nil {
		return apperr
	}

	session, apperr := createActiveSession(ctx, executionState, rsp.Token, rsp.Expiry)
	if apperr != nil {
		return apperr
	}

	// Create writers to capture command outputs
	outWriter := tangentcommon.NewBufferedWriter()
	errWriter := tangentcommon.NewBufferedWriter()

	apperr = session.Run(ctx, "", session.context.Skill, session.context.InputArgs, &tangentcommon.IOWriters{
		Out: outWriter,
		Err: errWriter,
	})

	fmt.Printf("outWriter: %s", outWriter.String())
	fmt.Printf("errWriter: %s", errWriter.String())

	if apperr != nil {
		return apperr
	}
	return nil
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
		serverURL:   "http://localhost:8080",
	})

	opts := httpclient.RequestOptions{
		Method: http.MethodGet,
		Path:   "sessions/execution-state",
	}

	body, _, err := client.DoRequest(opts)
	if err != nil {
		return nil, ErrFailedRequestToTansiveServer.Msg("unable to get execution state: " + err.Error())
	}

	executionState := &srvsession.ExecutionState{}
	if err := json.Unmarshal(body, executionState); err != nil {
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
		return nil, err
	}

	return session, nil
}
