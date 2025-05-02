package session

import (
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

// MessageReader represents something that can read complete message frames
type MessageReader interface {
	ReadMessage() ([]byte, error)
}

// MessageWriter represents something that can write complete message frames
type MessageWriter interface {
	Pause()
	Resume()
	WriteMessage([]byte) error
}

type chanMessageReader struct {
	ch <-chan []byte
}

type messageBuffer struct {
	data   []byte
	size   int
	marker string
}
type chanMessageWriter struct {
	mu           sync.Mutex // mutex to protect access to the buffer
	paused       bool
	buffer       []messageBuffer // buffered messages
	bufferedSize int             // bytes of buffered messages
	ch           chan<- []byte
}

func (r *chanMessageReader) ReadMessage() ([]byte, error) {
	msg, ok := <-r.ch
	if !ok {
		return nil, errors.New("channel closed")
	}
	return msg, nil
}

const maxBufferedBytes = 1024 * 1024 // Maximum bytes to buffer

func (w *chanMessageWriter) WriteMessage(msg []byte) error {
	defer func() {
		if r := recover(); r != nil {
			// Handle the panic gracefully
			log.Error().Msgf("Recovered from panic in WriteMessage: %v", r)
		}
	}()
	// Add to buffer if the message has a marker
	result := gjson.GetBytes(msg, "result.marker")
	if result.Exists() {
		id := result.String()

		msgLen := len(msg)
		if msgLen == 0 {
			return nil
		}
		shouldWrite := true
		w.mu.Lock()
		defer w.mu.Unlock()
		for w.bufferedSize+msgLen > maxBufferedBytes {
			// Evict oldest (pop from front)
			if len(w.buffer) == 0 {
				// don't write this message
				shouldWrite = false
				break
			}
			w.bufferedSize -= w.buffer[0].size
			w.buffer = w.buffer[1:]
		}
		if shouldWrite {
			m := messageBuffer{
				data:   msg,
				size:   msgLen,
				marker: id,
			}
			w.buffer = append(w.buffer, m)
			w.bufferedSize += len(msg)
		}
	}
	// If paused, do not send
	if w.paused || w.ch == nil {
		return nil
	}

	// Try to send latest message
	select {
	case w.ch <- msg:
		return nil
	default:
		return nil
	}
}

func (w *chanMessageWriter) Pause() {
	w.paused = true
}

func (w *chanMessageWriter) Resume() {
	w.paused = false
}

func (w *chanMessageWriter) Flush(marker string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ch == nil {
		return errors.New("channel is closed")
	}
	// If paused, do not flush
	if w.paused {
		return nil
	}
	// Flush all messages after the given marker
	// We intentionally hold the lock while flushing to prevent concurrent writes. Notifications are ok
	// to slip through.
	flushDeadline := time.After(3 * time.Second) // Set a deadline for flushing messages
	for i := 0; i < len(w.buffer); i++ {
		if w.buffer[i].marker == marker {
			// Send all messages after this marker
			for j := i + 1; j < len(w.buffer); j++ {
				if w.buffer[j].size > 0 {
					select {
					case w.ch <- w.buffer[j].data:
					case <-time.After(1 * time.Second):
						log.Error().Msg("Failed to send message after flush timeout")
						return errors.New("failed to send message after flush timeout")

					case <-flushDeadline:
						log.Error().Msg("Flush deadline exceeded")
						return errors.New("flush deadline exceeded")
					}
				}
			}
			break
		}
	}
	return nil
}

// When stop is called, there should be no writer and therefore needs no lock
func (w *chanMessageWriter) Stop() {
	// free the buffer
	w.buffer = nil
	close(w.ch)
	w.ch = nil
}

// NewChannelMessageReader creates a MessageReader from a receive-only channel
func NewChannelMessageReader(ch <-chan []byte) MessageReader {
	return &chanMessageReader{ch: ch}
}

// NewChannelMessageWriter creates a MessageWriter from a send-only channel
func NewChannelMessageWriter(ch chan<- []byte) MessageWriter {
	return &chanMessageWriter{ch: ch}
}
