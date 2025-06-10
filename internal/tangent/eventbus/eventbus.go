package eventbus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Topic string
	Data  any
}

type Subscriber struct {
	ID         string
	Topic      string
	BufferSize int
	Channel    chan Event
	Context    context.Context
	Cancel     context.CancelFunc

	mu     sync.Mutex
	closed bool
}

func (s *Subscriber) SafeSend(event Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	select {
	case s.Channel <- event:
		return true
	default:
		return false
	}
}

func (s *Subscriber) TimedSend(event Event, timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	select {
	case s.Channel <- event:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		s.Cancel()
		close(s.Channel)
	}
}

type EventBus struct {
	sync.RWMutex
	subscribers map[string]map[string]*Subscriber // topic -> subscriberID -> Subscriber
	counter     uint64
}

func New() *EventBus {
	return &EventBus{
		subscribers: make(map[string]map[string]*Subscriber),
	}
}

// Subscribe creates a new subscriber for the given topic and returns the event channel and unsubscribe function.
func (bus *EventBus) Subscribe(topic string, bufferSize int) (<-chan Event, func()) {
	id := fmt.Sprintf("sub-%d", atomic.AddUint64(&bus.counter, 1))

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan Event, bufferSize)

	sub := &Subscriber{
		ID:         id,
		Topic:      topic,
		BufferSize: bufferSize,
		Channel:    ch,
		Context:    ctx,
		Cancel:     cancel,
	}

	bus.Lock()
	defer bus.Unlock()

	if _, ok := bus.subscribers[topic]; !ok {
		bus.subscribers[topic] = make(map[string]*Subscriber)
	}
	bus.subscribers[topic][id] = sub

	unsubscribe := func() {
		bus.Lock()
		defer bus.Unlock()

		if subMap, ok := bus.subscribers[topic]; ok {
			if s, ok := subMap[id]; ok {
				s.Close()
				delete(subMap, id)
				if len(subMap) == 0 {
					delete(bus.subscribers, topic)
				}
			}
		}
	}

	return ch, unsubscribe
}

// CloseTopic removes all subscribers for a given topic.
func (bus *EventBus) CloseTopic(topic string) {
	bus.Lock()
	defer bus.Unlock()

	if subs, ok := bus.subscribers[topic]; ok {
		for _, sub := range subs {
			sub.Close()
		}
		delete(bus.subscribers, topic)
	}
}

// Publish sends an event to all subscribers of a topic.
// Non-blocking; will drop events for slow subscribers.
func (bus *EventBus) Publish(topic string, data any, timeout time.Duration) {
	event := Event{Topic: topic, Data: data}

	bus.RLock()
	defer bus.RUnlock()

	for pattern, subMap := range bus.subscribers {
		if matchTopic(pattern, topic) {
			for _, sub := range subMap {
				select {
				case <-sub.Context.Done():
					continue
				default:
					sub.TimedSend(event, timeout)
				}
			}
		}
	}
}

// Shutdown gracefully closes all subscribers and clears the bus.
func (bus *EventBus) Shutdown() {
	bus.Lock()
	defer bus.Unlock()

	for _, subs := range bus.subscribers {
		for _, sub := range subs {
			sub.Close()
		}
	}
	bus.subscribers = make(map[string]map[string]*Subscriber)
}

func matchTopic(pattern, topic string) bool {
	if pattern == "" || topic == "" {
		return false
	}
	if pattern == "*" || pattern == topic {
		return true
	}
	patternParts := strings.Split(pattern, ".")
	topicParts := strings.Split(topic, ".")

	if len(patternParts) != len(topicParts) {
		return false
	}

	for i := 0; i < len(patternParts); i++ {
		if patternParts[i] == "*" {
			continue
		}
		if patternParts[i] != topicParts[i] {
			return false
		}
	}
	return true
}
