package eventlogger

import (
	"encoding/base64"
	"io"

	"github.com/rs/zerolog"
)

type base64WrappedLogger struct {
	Logger zerolog.Logger
}

func (l *base64WrappedLogger) Write(p []byte) (n int, err error) {
	encoded := base64.StdEncoding.EncodeToString(p)
	l.Logger.Write([]byte(encoded))
	return len(p), nil
}

func NewBase64WrappedLogger(logger zerolog.Logger) io.Writer {
	return &base64WrappedLogger{Logger: logger}
}
