package session

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
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

	return nil, nil
}

func handleInteractiveSession(ctx context.Context, req *tangentcommon.SessionCreateRequest) apperrors.Error {
	// Create HTTP client with hardcoded server URL
	client := getHTTPClient(&clientConfig{
		token:       "some-token",
		tokenExpiry: time.Now().Add(1 * time.Hour),
		serverURL:   "http://localhost:8080",
	})

	// Make request to execution-state endpoint
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
		return apperrors.New("failed to make request to execution-state endpoint").Err(err)
	}

	// Log the response
	log.Ctx(ctx).Info().RawJSON("response", body).Msg("Received response from execution-state endpoint")

	return nil
}
