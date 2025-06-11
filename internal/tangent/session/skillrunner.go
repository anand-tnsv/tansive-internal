package session

import (
	"context"
	"encoding/json"

	"github.com/rs/zerolog/log"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/session/skillservice"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
	"github.com/tansive/tansive-internal/pkg/api"
)

func CreateSkillService() apperrors.Error {
	// Create and register the skill service
	skillService := skillservice.NewSkillService(&skillRunner{})

	go func() {
		err := skillService.StartServer()
		if err != nil {
			log.Error().Err(err).Msg("failed to start skill service server")
		}
	}()

	return nil
}

type skillRunner struct{}

func (s *skillRunner) GetTools(ctx context.Context, sessionID string) ([]api.LLMTool, apperrors.Error) {
	sessionUUID, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, ErrSessionError.Msg("invalid sessionID")
	}
	session, err := ActiveSessionManager().GetSession(sessionUUID)
	if err != nil {
		return nil, ErrSessionError.Msg(err.Error())
	}
	return session.getSkillsAsLLMTools()
}

func (s *skillRunner) GetContext(ctx context.Context, sessionID string, name string) (any, apperrors.Error) {
	sessionUUID, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, ErrSessionError.Msg("invalid sessionID")
	}
	session, err := ActiveSessionManager().GetSession(sessionUUID)
	if err != nil {
		return nil, ErrSessionError.Msg(err.Error())
	}
	return session.getContext(name)
}

func (s *skillRunner) Run(ctx context.Context, params *tangentcommon.RunParams) (map[string]any, apperrors.Error) {
	if params == nil {
		return nil, ErrSessionError.Msg("params is nil")
	}

	if params.SessionID == "" {
		return nil, ErrSessionError.Msg("sessionID is empty")
	}

	if params.SkillName == "" {
		return nil, ErrSessionError.Msg("skillName is empty")
	}

	if params.InvocationID == "" {
		return nil, ErrSessionError.Msg("invocationID is empty")
	}

	// Get the session
	sessionID, err := uuid.Parse(params.SessionID)
	if err != nil {
		return nil, ErrSessionError.Msg(err.Error())
	}

	session, err := ActiveSessionManager().GetSession(sessionID)
	if err != nil {
		return nil, ErrSessionError.Msg(err.Error())
	}

	// Create writers to capture command outputs
	outWriter := tangentcommon.NewBufferedWriter()
	errWriter := tangentcommon.NewBufferedWriter()

	// Run the skill
	apperr := session.Run(ctx, params.InvocationID, params.SkillName, params.InputArgs, &tangentcommon.IOWriters{
		Out: outWriter,
		Err: errWriter,
	})

	return processOutput(outWriter, errWriter, apperr)
}

func processOutput(outWriter *tangentcommon.BufferedWriter, errWriter *tangentcommon.BufferedWriter, err apperrors.Error) (map[string]any, apperrors.Error) {
	response := make(map[string]any)

	if err != nil {
		if err == ErrBlockedByPolicy {
			response["error"] = "This operation is blocked by Tansive policy. Please contact the administrator of your Tansive system to request access."
		} else {
			response["error"] = err.Error()
		}
		if errWriter.Len() > 0 {
			response["content"] = map[string]any{
				"type":  "text",
				"value": errWriter.String(),
			}
		}
		return response, err
	}

	output := outWriter.Bytes()
	var parsed any
	if json.Unmarshal(output, &parsed) == nil {
		response["content"] = map[string]any{
			"type":  detectJSONType(parsed),
			"value": parsed,
		}
	} else {
		// Not JSON, treat as plaintext
		response["content"] = map[string]any{
			"type":  "text",
			"value": outWriter.String(),
		}
	}

	return response, nil
}

func detectJSONType(v any) string {
	switch v := v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case nil:
		return "null"
	default:
		_ = v
		return "unknown"
	}
}
