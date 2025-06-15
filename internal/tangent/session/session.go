package session

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	srvsession "github.com/tansive/tansive-internal/internal/catalogsrv/session"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/eventlogger"
	"github.com/tansive/tansive-internal/internal/tangent/runners"
	"github.com/tansive/tansive-internal/internal/tangent/session/toolgraph"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
	"github.com/tansive/tansive-internal/pkg/api"
	"github.com/tansive/tansive-internal/pkg/types"
)

type session struct {
	id               uuid.UUID
	context          *ServerContext
	skillSet         catalogmanager.SkillSetManager
	viewDef          *policy.ViewDefinition
	token            string
	tokenExpiry      time.Time
	callGraph        *toolgraph.CallGraph
	invocationIDs    map[string]*policy.ViewDefinition
	auditLogger      zerolog.Logger
	auditLogComplete chan string
}

func (s *session) GetSessionID() string {
	return s.id.String()
}

func (s *session) Run(ctx context.Context, invokerID string, skillName string, inputArgs map[string]any, ioWriters ...*tangentcommon.IOWriters) apperrors.Error {
	log.Ctx(ctx).Info().Msgf("requested skill: %s", skillName)
	invocationID := uuid.New().String()
	s.auditLogger.Info().
		Str("event", "skill_start").
		Str("invoker_id", invokerID).
		Str("invocation_id", invocationID).
		Str("skill", skillName).
		Any("input_args", inputArgs).
		Msg("requested skill")
	if invokerID != "" {
		if _, ok := s.invocationIDs[invokerID]; !ok {
			log.Ctx(ctx).Error().Str("invocation_id", invokerID).Msg("invocationID not found")
			return ErrInvalidInvocationID.Msg("invocationID not found")
		}
	}

	if err := s.fetchObjects(ctx); err != nil {
		return err
	}

	isAllowed, basis, actions, err := s.ValidateRunPolicy(ctx, invokerID, skillName)
	if err != nil {
		return err
	}
	if !isAllowed {
		msg := fmt.Sprintf("blocked by Tansive policy: view '%s' does not authorize any of required actions - %v - to use this skill", s.context.View, actions)
		log.Ctx(ctx).Error().Str("policy_decision", "true").Msg(msg)
		s.auditLogger.Error().
			Str("event", "policy_decision").
			Str("decision", "blocked").
			Str("invocation_id", invocationID).
			Str("view", s.context.View).
			Any("basis", basis).
			Str("skill", skillName).
			Any("actions", actions).
			Msg("blocked by policy")
		return ErrBlockedByPolicy.Msg(msg)
	}
	msg := fmt.Sprintf("allowed by Tansive policy: view '%s' authorizes actions - %v - to use this skill", s.context.View, actions)
	log.Ctx(ctx).Info().Str("policy_decision", "true").Msg(msg)
	s.auditLogger.Info().
		Str("event", "policy_decision").
		Str("decision", "allowed").
		Str("invocation_id", invocationID).
		Str("view", s.context.View).
		Any("basis", basis).
		Str("skill", skillName).
		Any("actions", actions).
		Msg("allowed by policy")

	if err := s.ValidateInputForSkill(ctx, skillName, inputArgs); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("invalid input for skill")
		return err
	}

	// We only support interactive skills for now
	err = s.runInteractiveSkill(ctx, invokerID, invocationID, skillName, inputArgs, ioWriters...)

	if err != nil {
		s.auditLogger.Error().
			Str("event", "skill_end").
			Str("status", "failed").
			Str("invocation_id", invocationID).
			Err(err).
			Str("skill", skillName).
			Msg("skill completed")
	} else {
		s.auditLogger.Info().
			Str("event", "skill_end").
			Str("status", "success").
			Str("invocation_id", invocationID).
			Str("skill", skillName).
			Msg("skill completed")
	}
	return err
}

func (s *session) ValidateRunPolicy(ctx context.Context, invokerID string, skillName string) (bool, map[policy.Intent][]policy.Rule, []string, apperrors.Error) {
	if s.skillSet == nil {
		log.Ctx(ctx).Error().Msg("skillSet not found")
		return false, nil, nil, ErrUnableToGetSkillset.Msg("skillset not found")
	}

	skill, err := s.resolveSkill(skillName)
	if err != nil {
		return false, nil, nil, err
	}

	actions := []string{}
	allowed, basis, err := policy.AreActionsAllowedOnResource(s.viewDef, s.skillSet.GetResourcePath(), skill.GetExportedActions())
	if err != nil {
		return false, nil, nil, err
	}
	for _, action := range skill.GetExportedActions() {
		actions = append(actions, string(action))
	}

	return allowed, basis, actions, nil
}

func (s *session) ValidateInputForSkill(ctx context.Context, skillName string, inputArgs map[string]any) apperrors.Error {
	skill, err := s.resolveSkill(skillName)
	if err != nil {
		return err
	}
	return skill.ValidateInput(inputArgs)
}

func (s *session) runInteractiveSkill(ctx context.Context, invokerID, invocationID string, skillName string, inputArgs map[string]any, ioWriters ...*tangentcommon.IOWriters) apperrors.Error {
	if s.skillSet == nil {
		return ErrUnableToGetSkillset.Msg("skillset not found")
	}

	skill, err := s.resolveSkill(skillName)
	if err != nil {
		return err
	}
	if err := skill.ValidateInput(inputArgs); err != nil {
		return err
	}

	runner, err := s.getRunner(ctx, skillName, ioWriters...)
	if err != nil {
		return err
	}

	interactiveIOWriters := &tangentcommon.IOWriters{
		Out: s.getLogger(TopicInteractiveLog).With().Str("actor", "skill").Str("source", "stdout").Str("runner", runner.ID()).Str("skill", skillName).Logger(),
		Err: s.getLogger(TopicInteractiveLog).With().Str("actor", "skill").Str("source", "stderr").Str("runner", runner.ID()).Str("skill", skillName).Logger(),
	}

	runner.AddWriters(interactiveIOWriters)

	serviceEndpoint, goerr := api.GetSocketPath()
	if goerr != nil {
		return ErrUnableToGetSkillset.Msg("failed to get socket path")
	}
	// create the arguments
	args := api.SkillInputArgs{
		InvocationID:     invocationID,
		ServiceEndpoint:  serviceEndpoint,
		RunMode:          api.RunModeInteractive,
		SessionID:        s.id.String(),
		SkillName:        skillName,
		InputArgs:        inputArgs,
		SessionVariables: s.context.SessionVariables,
	}

	toolErr := s.callGraph.RegisterCall(toolgraph.CallID(invokerID), toolgraph.ToolName(skillName), toolgraph.CallID(invocationID))
	if toolErr != nil {
		return ErrToolGraphError.Msg(toolErr.Error())
	}
	s.invocationIDs[invocationID] = s.viewDef

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := make(chan apperrors.Error, 1)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(ctx context.Context) {
		defer wg.Done()
		defer cancel()

		ctx = log.Ctx(ctx).With().Str("runner", runner.ID()).Str("actor", "runner").Logger().WithContext(ctx)
		log.Ctx(ctx).Info().Msgf("running skill: %s", skillName)
		s.auditLogger.Info().
			Str("event", "runner_start").
			Str("runner", runner.ID()).
			Str("invocation_id", invocationID).
			Str("skill", skillName).
			Msg("starting runner")
		err := runner.Run(ctx, &args)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msgf("error running skill: %s", skillName)
			s.auditLogger.Error().
				Str("event", "runner_completed").
				Str("status", "failed").
				Str("invocation_id", invocationID).
				Err(err).
				Str("runner", runner.ID()).
				Str("skill", skillName).
				Msg("runner completed")
			resultChan <- err
		} else {
			log.Ctx(ctx).Info().Msgf("skill completed successfully: %s", skillName)
			s.auditLogger.Info().
				Str("event", "runner_completed").
				Str("status", "success").
				Str("invocation_id", invocationID).
				Str("runner", runner.ID()).
				Str("skill", skillName).
				Msg("runner completed")
			resultChan <- nil
		}

	}(childCtx)

	// Set up a graceful exit to allow for draining logs/event bus before exiting
	gracefulExitChan := make(chan struct{}, 1)
	var once sync.Once

	go func() {
		select {
		case <-childCtx.Done():
		case <-ctx.Done():
			cancel()
		}

		time.AfterFunc(100*time.Millisecond, func() {
			once.Do(func() {
				gracefulExitChan <- struct{}{}
			})
		})
	}()

	<-gracefulExitChan
	log.Info().Msg("interactive skill exited")

	wg.Wait()

	return <-resultChan
}

func (s *session) getRunner(ctx context.Context, skillName string, ioWriters ...*tangentcommon.IOWriters) (runners.Runner, apperrors.Error) {
	if s.skillSet == nil {
		return nil, ErrUnableToGetSkillset.Msg("skillset not found")
	}

	runnerDef, err := s.skillSet.GetRunnerDefinitionForSkill(skillName)
	if err != nil {
		return nil, err
	}
	runner, err := runners.NewRunner(ctx, s.id.String(), runnerDef, ioWriters...)
	if err != nil {
		return nil, err
	}

	return runner, nil
}

func (s *session) fetchObjects(ctx context.Context) apperrors.Error {
	client := getHTTPClient(&clientConfig{
		token:       s.token,
		tokenExpiry: s.tokenExpiry,
		serverURL:   config.Config().TansiveServer.GetURL(),
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
	s.viewDef = s.context.ViewDefinition

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

func (s *session) getLogger(eventType string) zerolog.Logger {
	return eventlogger.NewLogger(GetEventBus(), s.getTopic(eventType)).With().Str("session_id", s.id.String()).Logger()
}

func (s *session) getTopic(eventType string) string {
	return GetSessionTopic(s.id.String(), eventType)
}

func (s *session) getSkillsAsLLMTools() ([]api.LLMTool, apperrors.Error) {
	if s.skillSet == nil {
		return nil, ErrUnableToGetSkillset.Msg("skillset not found")
	}
	// We'll return all tools and block it while executing the skill. This will allow LLM to prompt
	// user to obtain permission or to log tickets to ask for permission.
	return s.skillSet.GetAllSkillsAsLLMTools(nil), nil
}

func (s *session) getContext(name string) (any, apperrors.Error) {
	if s.skillSet == nil {
		return nil, ErrUnableToGetSkillset.Msg("skillset not found")
	}
	return s.skillSet.GetContextValue(name)
}

var _ = (&session{}).setContext

func (s *session) setContext(name string, value any) apperrors.Error {
	if s.skillSet == nil {
		return ErrUnableToGetSkillset.Msg("skillset not found")
	}
	nullableAny, err := types.NullableAnyFrom(value)
	if err != nil {
		return ErrInvalidObject.Msg(err.Error())
	}
	return s.skillSet.SetContextValue(name, nullableAny)
}

func (s *session) Finalize(ctx context.Context, apperr apperrors.Error) apperrors.Error {
	auditLogPath := ""
	auditLog := ""

	select {
	case auditLogPath = <-s.auditLogComplete:
		log.Ctx(ctx).Info().Str("audit_log_path", auditLogPath).Msg("audit log complete")
	case <-time.After(10 * time.Second):
		log.Ctx(ctx).Error().Msg("audit log not complete after 10 seconds")
	}

	if auditLogPath != "" {
		var err error
		auditLog, err = srvsession.CompressAndEncodeAuditLogFile(auditLogPath)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to compress and encode audit log")
		}
	}

	sessionStatus := srvsession.ExecutionStatusUpdate{
		StatusSummary: srvsession.SessionStatusCompleted,
		Status: srvsession.ExecutionStatus{
			AuditLog: auditLog,
		},
	}
	if apperr != nil {
		sessionStatus.StatusSummary = srvsession.SessionStatusFailed
		sessionStatus.Status.Error = map[string]any{
			"message": apperr.Error(),
		}
	}

	client := getHTTPClient(&clientConfig{
		token:       s.token,
		tokenExpiry: s.tokenExpiry,
		serverURL:   config.Config().TansiveServer.GetURL(),
	})

	body, err := json.Marshal(sessionStatus)
	if err != nil {
		return ErrFailedRequestToTansiveServer.Msg(err.Error())
	}

	opts := httpclient.RequestOptions{
		Method: http.MethodPut,
		Path:   "sessions/execution-state",
		Body:   body,
	}

	_, _, err = client.DoRequest(opts)
	if err != nil {
		return ErrFailedRequestToTansiveServer.Msg(err.Error())
	}

	return nil
}
