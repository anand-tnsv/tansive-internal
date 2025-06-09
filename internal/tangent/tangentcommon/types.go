package tangentcommon

import "io"

// IOWriters provides stdout and stderr writers for command output.
// Both Out and Err must implement io.Writer.
type IOWriters struct {
	Out io.Writer // stdout writer, must implement io.Writer
	Err io.Writer // stderr writer, must implement io.Writer
}
