package httpx

import "net/http"

// ResponseWriter is a wrapper around http.ResponseWriter that tracks if headers were written
type ResponseWriter struct {
	http.ResponseWriter
	written bool
}

// NewResponseWriter creates a new ResponseWriter
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w}
}

// WriteHeader implements http.ResponseWriter
func (rw *ResponseWriter) WriteHeader(code int) {
	rw.written = true
	rw.ResponseWriter.WriteHeader(code)
}

// Write implements http.ResponseWriter
func (rw *ResponseWriter) Write(b []byte) (int, error) {
	rw.written = true
	return rw.ResponseWriter.Write(b)
}

// Written returns true if headers or body were written
func (rw *ResponseWriter) Written() bool {
	return rw.written
}
