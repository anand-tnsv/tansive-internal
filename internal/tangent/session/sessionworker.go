package session

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/policy"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/tangent/eventlogger"
)

func (s *session) Run(ctx context.Context) apperrors.Error {
	if err := s.FetchObjects(ctx); err != nil {
		return err
	}

	// TODO: run the skill
	return nil
}

func (s *session) RunInteractiveSkill(ctx context.Context) apperrors.Error {
	log.Ctx(ctx).Info().Msg("Running interactive skill")

	// get command io writers
	ioWriters := &commandIOWriters{
		out: s.getLogger(TopicInteractiveLog).With().Str("source", "stdout").Str("runner", "shell").Logger(),
		err: s.getLogger(TopicInteractiveLog).With().Str("source", "stderr").Str("runner", "shell").Logger(),
	}

	sessionLog, unsubSessionLog := GetEventBus().Subscribe(s.getTopic(TopicSessionLog), 100)
	defer unsubSessionLog()
	interactiveLog, unsubInteractiveLog := GetEventBus().Subscribe(s.getTopic(TopicInteractiveLog), 100)
	defer unsubInteractiveLog()

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(log zerolog.Logger) {
		defer wg.Done()
		defer cancel()

		err := shellCmd(childCtx, "ping 8.8.8.8", &shellConfig{
			dir: "/Users/anandm/tansive/tansive-internal",
		}, ioWriters)
		if err != nil {
			log.Error().Err(err).Msg("Error running shell command")
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

	return nil
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
