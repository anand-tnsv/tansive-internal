package session

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type AsciinemaEvent struct {
	Time   float64 `json:"time"`
	Stream string  `json:"stream"` // "o" or "i"
	Data   string  `json:"data"`
}

type AsciinemaWriter struct {
	file   *os.File
	writer *bufio.Writer
	start  time.Time
	closed bool
}

// Open creates a new asciinema file and writes the header
func NewAsciinemaWriter(filename string) (*AsciinemaWriter, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	w := bufio.NewWriter(f)

	// Write the Asciinema v2 header
	header := map[string]any{
		"version":   2,
		"width":     80,
		"height":    24,
		"timestamp": time.Now().Unix(),
		"env": map[string]string{
			"SHELL": os.Getenv("SHELL"),
			"TERM":  "xterm-256color",
		},
	}
	headerBytes, _ := json.Marshal(header)
	fmt.Fprintln(w, string(headerBytes))

	return &AsciinemaWriter{
		file:   f,
		writer: w,
		start:  time.Now(),
	}, nil
}

// Write appends a new event (either input or output) with a timestamp
func (a *AsciinemaWriter) Write(stream string, data string) error {
	if a.closed {
		return fmt.Errorf("asciinema writer already closed")
	}
	elapsed := time.Since(a.start).Seconds()
	event := []any{elapsed, stream, data}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(a.writer, string(eventBytes))
	return err
}

// Close flushes and closes the underlying file
func (a *AsciinemaWriter) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	if err := a.writer.Flush(); err != nil {
		return err
	}
	return a.file.Close()
}
