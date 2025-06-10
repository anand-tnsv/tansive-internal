package session

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/session/skillservice"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
)

func CreateSkillService() apperrors.Error {
	server, err := skillservice.NewServer()
	if err != nil {
		log.Error().Err(err).Msg("failed to create skill service server")
		return ErrSessionError.Msg(err.Error())
	}
	// Create and register the skill service
	skillService := skillservice.NewSkillService(&skillRunner{})
	server.RegisterService(skillService)

	go func() {
		err := server.Start()
		if err != nil {
			log.Error().Err(err).Msg("failed to start skill service server")
		}
	}()

	return nil
}

type skillRunner struct{}

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
		response["error"] = err.Error()
		if errWriter.Len() > 0 {
			response["message"] = errWriter.String()
		}
	} else {
		response["output"] = outWriter.String()
	}
	return response, err
}
