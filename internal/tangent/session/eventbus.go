package session

import (
	"fmt"

	"github.com/tansive/tansive-internal/internal/tangent/eventbus"
)

var eventBus *eventbus.EventBus

const (
	TopicInteractiveLog = "interactive.log"
	TopicAuditLog       = "audit.log"
	TopicSessionLog     = "session.log"
)

func init() {
	eventBus = eventbus.New()
	if eventBus == nil {
		panic("eventBus is nil")
	}
}

func GetEventBus() *eventbus.EventBus {
	return eventBus
}

func GetAllSessionTopics(sessionID string) string {
	return fmt.Sprintf("session.%s.*", sessionID)
}

func GetSessionTopic(sessionID string, topic string) string {
	return fmt.Sprintf("session.%s.%s", sessionID, topic)
}
