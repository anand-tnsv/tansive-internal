package session

import "github.com/tansive/tansive-internal/internal/tangent/eventbus"

var eventBus *eventbus.EventBus

func init() {
	eventBus = eventbus.New()
	if eventBus == nil {
		panic("eventBus is nil")
	}
}

func GetEventBus() *eventbus.EventBus {
	return eventBus
}
