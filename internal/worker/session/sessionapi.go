package session

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tansive/tansive-internal/internal/common/httpx"
	"github.com/tansive/tansive-internal/internal/common/uuidv7utils"
	"github.com/tansive/tansive-internal/internal/worker/session/api"
)

// Create a new resource object
func createSession(r *http.Request) (*httpx.Response, error) {
	req := api.CreateSessionRequest{}

	// Decode JSON body
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, ErrBadRequest.Msg("failed to decode request body")
	}
	sessionID, err := uuid.Parse(req.ID)
	if err != nil {
		return nil, ErrInvalidSession.Msg("invalid session ID")
	}

	if !uuidv7utils.IsUUIDv7(sessionID) {
		return nil, ErrInvalidSession.Msg("session ID must be a valid UUIDv7")
	}

	if err := ActiveSessionManager().CreateSession(sessionID, &session{
		id:      sessionID,
		context: req.Context,
	}); err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusCreated,
		Location:   "/sessions/" + req.ID,
		Response:   nil,
	}
	return rsp, nil
}

// Get a session by ID
func getSession(r *http.Request) (*httpx.Response, error) {
	id := chi.URLParam(r, "id")
	sessionID, err := uuid.Parse(id)
	if err != nil {
		return nil, ErrInvalidSession.Msg("invalid session ID")
	}

	session, err := ActiveSessionManager().GetSession(sessionID)
	if err != nil {
		return nil, err
	}

	rsp := &api.GetSessionResponse{
		Session: api.Session{
			ID:      session.id.String(),
			Context: session.context,
		},
	}
	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   rsp,
	}, nil
}

func listSessions(r *http.Request) (*httpx.Response, error) {
	sessions, err := ActiveSessionManager().ListSessions()
	if err != nil {
		return nil, err
	}

	rsp := &api.ListSessionsResponse{
		Sessions: make([]api.Session, len(sessions)),
	}
	for i, session := range sessions {
		rsp.Sessions[i] = api.Session{
			ID:      session.id.String(),
			Context: session.context,
		}
	}

	return &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   rsp,
	}, nil
}

func deleteSession(r *http.Request) (*httpx.Response, error) {
	id := chi.URLParam(r, "id")
	sessionID, err := uuid.Parse(id)
	if err != nil {
		return nil, ErrInvalidSession.Msg("invalid session ID")
	}

	if err := ActiveSessionManager().DeleteSession(sessionID); err != nil {
		return nil, err
	}

	return &httpx.Response{
		StatusCode: http.StatusNoContent,
		Response:   nil,
	}, nil
}
