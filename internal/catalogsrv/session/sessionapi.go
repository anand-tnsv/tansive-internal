package session

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/auth"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type InteractiveSessionRsp struct {
	Code       string `json:"code"`
	TangentURL string `json:"tangent_url"`
}

type SessionTokenRsp struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

type ExecutionState struct {
	SessionID        uuid.UUID              `json:"session_id"`
	SkillSet         string                 `json:"skillset"`
	Skill            string                 `json:"skill"`
	View             string                 `json:"view"`
	ViewDefinition   *policy.ViewDefinition `json:"view_definition"`
	SessionVariables map[string]any         `json:"session_variables"`
	InputArgs        map[string]any         `json:"input_args"`
	Catalog          string                 `json:"catalog"`
	Variant          string                 `json:"variant"`
	Namespace        string                 `json:"namespace"`
	TenantID         catcommon.TenantId     `json:"tenant_id"`
}

func newSession(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest("request body is required")
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	interactive := r.URL.Query().Get("interactive") == "true"
	codeChallenge := ""
	if interactive {
		//We need to create a oauth2.0 session, so look for a code challenge
		codeChallenge = r.URL.Query().Get("code_challenge")
		if codeChallenge == "" {
			return nil, httpx.ErrInvalidRequest("code_challenge is required")
		}
	}

	session, tangent, err := NewSession(ctx, req, WithInteractive(interactive), WithCodeChallenge(codeChallenge))
	if err != nil {
		return nil, err
	}

	session.Save(ctx)

	if interactive {
		log.Ctx(ctx).Info().Msgf("Creating auth code for session %s", session.ID().String())
		authCode, err := CreateAuthCode(ctx, session.ID(), codeChallenge)
		if err != nil {
			return nil, err
		}

		resp := &httpx.Response{
			StatusCode: http.StatusOK,
			Response: &InteractiveSessionRsp{
				Code:       authCode,
				TangentURL: tangent.URL,
			},
		}
		return resp, nil
	}

	return &httpx.Response{
		StatusCode: http.StatusCreated,
		Location:   "/sessions/" + session.ID().String(),
		Response:   nil,
	}, nil
}

func getExecutionState(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	sessionID := chi.URLParam(r, "sessionID")
	sessionUUID, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, httpx.ErrInvalidRequest("invalid session ID")
	}
	session, err := GetSession(ctx, sessionUUID)
	if err != nil {
		return nil, err
	}
	executionState := session.GetExecutionState(ctx)
	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   executionState,
	}, nil
}

func createExecutionState(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	code := r.URL.Query().Get("code")
	if code == "" {
		return nil, httpx.ErrInvalidRequest("code is required")
	}
	codeVerifier := r.URL.Query().Get("code_verifier")
	if codeVerifier == "" {
		return nil, httpx.ErrInvalidRequest("code_verifier is required")
	}

	authCode, err := GetAuthCode(ctx, code, codeVerifier)
	if err != nil {
		return nil, ErrNotAuthorized
	}

	session, err := GetSession(ctx, authCode.SessionID)
	if err != nil {
		return nil, err
	}

	token, expiry, err := createSessionToken(ctx, session)
	if err != nil {
		return nil, err
	}

	session.SetStatusSummary(ctx, SessionStatusRunning)

	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response: &SessionTokenRsp{
			Token:  token,
			Expiry: expiry,
		},
	}, nil
}

var _ = createSessionToken

func createSessionToken(ctx context.Context, session SessionManager) (string, time.Time, error) {
	var view *models.View
	if vm, err := session.GetViewManager(ctx); err == nil {
		view, err = vm.GetViewModel()
		if err != nil {
			return "", time.Time{}, err
		}
		if view == nil {
			return "", time.Time{}, httpx.ErrInvalidRequest("view is nil")
		}
	} else {
		return "", time.Time{}, err
	}

	subjectType := catcommon.GetSubjectType(ctx)

	additionalClaims := map[string]any{
		"sub": "session/" + session.ID().String(),
	}

	if subjectType == catcommon.SubjectTypeUser {
		userID := catcommon.GetUserID(ctx)
		if userID == "" {
			return "", time.Time{}, httpx.ErrInvalidRequest("user ID is required")
		}
		additionalClaims["created_by"] = "user/" + userID
	}

	token, expiry, err := auth.CreateToken(ctx, view, auth.WithAdditionalClaims(additionalClaims))
	if err != nil {
		return "", time.Time{}, err
	}
	return token, expiry, nil
}
