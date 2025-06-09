package session

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/tangent/eventlogger"
	"github.com/tansive/tansive-internal/internal/tangent/runners"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
)

type SkillArgs struct {
	SessionID        string         `json:"sessionID"`
	SkillName        string         `json:"skillName"`
	InputArgs        map[string]any `json:"inputArgs"`
	SessionVariables map[string]any `json:"sessionVariables"`
}

func (s *session) Run(ctx context.Context, skillName string, inputArgs map[string]any) apperrors.Error {
	if err := s.FetchObjects(ctx); err != nil {
		return err
	}

	// We only support interactive skills for now
	return s.runInteractiveSkill(ctx, skillName, inputArgs)
}

func (s *session) runInteractiveSkill(ctx context.Context, skillName string, inputArgs map[string]any) apperrors.Error {
	log.Ctx(ctx).Info().Msg("Running interactive skill")
	if s.skillSet == nil {
		return ErrUnableToGetSkillset.Msg("skillset not found")
	}

	// get command io writers
	if s.interactiveIOWriters == nil {
		s.interactiveIOWriters = &tangentcommon.IOWriters{
			Out: s.getLogger(TopicInteractiveLog).With().Str("source", "stdout").Str("runner", "stdiorunner").Logger(),
			Err: s.getLogger(TopicInteractiveLog).With().Str("source", "stderr").Str("runner", "stdiorunner").Logger(),
		}
	}

	sessionLog, unsubSessionLog := GetEventBus().Subscribe(s.getTopic(TopicSessionLog), 100)
	defer unsubSessionLog()
	interactiveLog, unsubInteractiveLog := GetEventBus().Subscribe(s.getTopic(TopicInteractiveLog), 100)
	defer unsubInteractiveLog()

	skill, err := s.resolveSkill(skillName)
	if err != nil {
		return err
	}
	if err := skill.ValidateInput(inputArgs); err != nil {
		return err
	}

	runner, err := s.GetRunner(ctx, s.interactiveIOWriters)
	if err != nil {
		return err
	}

	// create the arguments
	args := SkillArgs{
		SessionID:        s.id.String(),
		SkillName:        skillName,
		InputArgs:        s.context.Info.InputArgs,
		SessionVariables: s.context.Info.SessionVariables,
	}
	argsMap := make(map[string]any)
	if err := mapstructure.Decode(args, &argsMap); err != nil {
		return ErrInvalidObject.Msg(err.Error())
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := make(chan apperrors.Error, 1)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(log zerolog.Logger) {
		defer wg.Done()
		defer cancel()

		log.Info().Msg("Running interactive skill: " + skillName)
		err := runner.Run(childCtx, argsMap)
		if err != nil {
			log.Error().Err(err).Msg("Error running shell command")
			resultChan <- err
		} else {
			log.Info().Msg("Interactive skill completed successfully")
			resultChan <- nil
		}

	}(s.getLogger(TopicSessionLog))

	// Set up a graceful exit to allow for draining logs/event bus before exiting
	gracefulExitChan := make(chan struct{}, 1)
	var once sync.Once

	go func() {
		select {
		case <-childCtx.Done():
		case <-ctx.Done():
			cancel()
		}

		log.Ctx(ctx).Info().Msg("Interactive skill exited")
		time.AfterFunc(1*time.Second, func() {
			once.Do(func() {
				gracefulExitChan <- struct{}{}
			})
		})
	}()

loop:
	for {
		select {
		case <-gracefulExitChan:
			log.Ctx(ctx).Info().Msg("Interactive skill exited")
			break loop
		case event := <-interactiveLog:
			fmt.Println(event.Data)
		case event := <-sessionLog:
			fmt.Println(event.Data)
		}
	}

	wg.Wait()

	return <-resultChan
}

func (s *session) GetRunner(ctx context.Context, ioWriters *tangentcommon.IOWriters) (runners.Runner, apperrors.Error) {
	if s.skillSet == nil {
		return nil, ErrUnableToGetSkillset.Msg("skillset not found")
	}

	runnerDef := s.skillSet.GetRunnerDefinition()
	runner, err := runners.NewRunner(ctx, s.id.String(), runnerDef, ioWriters)
	if err != nil {
		return nil, err
	}

	return runner, nil
}

func (s *session) FetchObjects(ctx context.Context) apperrors.Error {
	client := getHTTPClient(&clientConfig{
		token:       s.token,
		tokenExpiry: s.tokenExpiry,
		serverURL:   s.serverURL,
	})

	// get skillset
	if s.skillSet == nil && s.context.SkillSet != "" {
		skillset, err := getSkillset(ctx, client, s.context.SkillSet)
		if err != nil {
			return err
		}
		s.skillSet = skillset
	}

	// get view definition
	if s.viewDef == nil && len(s.context.ViewDefinition) > 0 {
		viewDef, err := getViewDefinition(ctx, s.context.ViewDefinition)
		if err != nil {
			return err
		}
		s.viewDef = viewDef
	}

	return nil
}

func (s *session) resolveSkill(skillName string) (*catalogmanager.Skill, apperrors.Error) {
	if s.skillSet == nil {
		return nil, ErrUnableToGetSkillset.Msg("skillset not found")
	}

	skill, err := s.skillSet.GetSkill(skillName)
	if err != nil {
		return nil, err
	}
	return &skill, nil
}

func getSkillset(ctx context.Context, client httpclient.HTTPClientInterface, skillset string) (catalogmanager.SkillSetManager, apperrors.Error) {
	response, err := client.GetResource(catcommon.KindNameSkillsets, skillset, nil, "")
	if err != nil {
		httpErr, ok := err.(*httpclient.HTTPError)
		if ok {
			return nil, ErrUnableToGetSkillset.Msg(httpErr.Message)
		}
		return nil, ErrUnableToGetSkillset.Msg(err.Error())
	}

	// create new skillset manager
	sm, err := catalogmanager.SkillSetManagerFromJSON(ctx, response)
	if err != nil {
		return nil, ErrUnableToGetSkillset.Msg(err.Error())
	}

	return sm, nil
}

func getViewDefinition(ctx context.Context, viewDefJSON []byte) (*policy.ViewDefinition, apperrors.Error) {
	_ = ctx
	viewDef := &policy.ViewDefinition{}
	err := json.Unmarshal(viewDefJSON, &viewDef)
	if err != nil {
		return nil, ErrUnableToGetViewDefinition.Msg(err.Error())
	}
	return viewDef, nil
}

func (s *session) getLogger(eventType string) zerolog.Logger {
	return eventlogger.NewLogger(GetEventBus(), s.getTopic(eventType)).With().Str("session_id", s.id.String()).Logger()
}

const (
	TopicInteractiveLog = "interactive.log"
	TopicAuditLog       = "audit.log"
	TopicSessionLog     = "session.log"
)

func (s *session) getTopic(eventType string) string {
	return fmt.Sprintf("session.%s.%s", s.id.String(), eventType)
}
