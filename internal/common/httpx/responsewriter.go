package httpx

import (
	"bufio"
	"io"
	"net"
	"net/http"
)

// ResponseWriter is a wrapper around http.ResponseWriter that tracks if headers were written
type ResponseWriter struct {
	http.ResponseWriter
	written bool
	status  int
}

// NewResponseWriter creates a new ResponseWriter
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w}
}

// WriteHeader implements http.ResponseWriter
func (rw *ResponseWriter) WriteHeader(code int) {
	if rw.written {
		// Do not override if already written
		return
	}
	rw.status = code
	rw.written = true
	rw.ResponseWriter.WriteHeader(code)
}

// Write implements http.ResponseWriter
func (rw *ResponseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}

// Written returns true if headers or body were written
func (rw *ResponseWriter) Written() bool {
	return rw.written
}

// Status returns the status code (default 200 if not set)
func (rw *ResponseWriter) Status() int {
	if rw.status == 0 {
		return http.StatusOK
	}
	return rw.status
}

// Flush implements http.Flusher if underlying writer supports it
func (rw *ResponseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker if underlying writer supports it
func (rw *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrHijacked
	}
	return hj.Hijack()
}

// CloseNotify implements http.CloseNotifier (deprecated, but still sometimes used)
func (rw *ResponseWriter) CloseNotify() <-chan bool {
	if cn, ok := rw.ResponseWriter.(http.CloseNotifier); ok {
		return cn.CloseNotify()
	}
	// dummy channel to prevent nil panic
	ch := make(chan bool, 1)
	close(ch)
	return ch
}

// Push implements http.Pusher if supported (for HTTP/2)
func (rw *ResponseWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := rw.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

// ReadFrom implements io.ReaderFrom if supported
func (rw *ResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := rw.ResponseWriter.(io.ReaderFrom); ok {
		if !rw.written {
			rw.WriteHeader(http.StatusOK)
		}
		return rf.ReadFrom(r)
	}
	// fallback to io.Copy
	return io.Copy(rw, r)
}
