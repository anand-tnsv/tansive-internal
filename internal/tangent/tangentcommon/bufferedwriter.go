package tangentcommon

import (
	"bytes"
)

// BufferedWriter is a simple buffer that accumulates all writes in memory.
type BufferedWriter struct {
	buf bytes.Buffer
}

// Write implements io.Writer
func (b *BufferedWriter) Write(p []byte) (int, error) {
	return b.buf.Write(p)
}

// WriteString implements io.StringWriter
func (b *BufferedWriter) WriteString(s string) (int, error) {
	return b.buf.WriteString(s)
}

// String returns the accumulated contents as a string.
func (b *BufferedWriter) String() string {
	return b.buf.String()
}

// Bytes returns the accumulated contents as a byte slice.
func (b *BufferedWriter) Bytes() []byte {
	return b.buf.Bytes()
}

// Reset clears the buffer.
func (b *BufferedWriter) Reset() {
	b.buf.Reset()
}

// Len returns the number of bytes in the buffer.
func (b *BufferedWriter) Len() int {
	return b.buf.Len()
}

// NewBufferedWriter constructs a new BufferedWriter.
func NewBufferedWriter() *BufferedWriter {
	return &BufferedWriter{}
}
