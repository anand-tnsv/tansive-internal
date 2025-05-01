package session

import (
	"errors"
)

// MessageReader represents something that can read complete message frames
type MessageReader interface {
	ReadMessage() ([]byte, error)
}

// MessageWriter represents something that can write complete message frames
type MessageWriter interface {
	WriteMessage([]byte) error
}

type chanMessageReader struct {
	ch <-chan []byte
}

type chanMessageWriter struct {
	ch chan<- []byte
}

func (r *chanMessageReader) ReadMessage() ([]byte, error) {
	msg, ok := <-r.ch
	if !ok {
		return nil, errors.New("channel closed")
	}
	return msg, nil
}

func (w *chanMessageWriter) WriteMessage(msg []byte) error {
	select {
	case w.ch <- msg:
		return nil
	default:
		return errors.New("channel write blocked or closed")
	}
}

// NewChannelMessageReader creates a MessageReader from a receive-only channel
func NewChannelMessageReader(ch <-chan []byte) MessageReader {
	return &chanMessageReader{ch: ch}
}

// NewChannelMessageWriter creates a MessageWriter from a send-only channel
func NewChannelMessageWriter(ch chan<- []byte) MessageWriter {
	return &chanMessageWriter{ch: ch}
}
