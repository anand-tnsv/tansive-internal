package eventlogger

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/tansive/tansive-internal/internal/tangent/eventbus"
)

// LogWriter is a zerolog-compatible writer that sends logs to an EventBus topic.
type LogWriter struct {
	Bus   *eventbus.EventBus
	Topic string
}

// Write publishes a log message to the specified topic on the EventBus.
func (lw *LogWriter) Write(p []byte) (n int, err error) {
	dup := make([]byte, len(p))
	copy(dup, p)
	lw.Bus.Publish(lw.Topic, dup, 100*time.Millisecond)
	return len(p), nil
}

// NewLogger creates a zerolog.Logger that publishes to the given EventBus topic.
func NewLogger(bus *eventbus.EventBus, topic string) zerolog.Logger {
	return zerolog.New(&LogWriter{
		Bus:   bus,
		Topic: topic,
	}).With().Timestamp().Logger()
}
