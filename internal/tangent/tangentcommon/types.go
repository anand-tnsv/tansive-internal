package tangentcommon

import (
	"context"
	"io"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

// IOWriters provides stdout and stderr writers for command output.
// Both Out and Err must implement io.Writer.
type IOWriters struct {
	Out io.Writer // stdout writer, must implement io.Writer
	Err io.Writer // stderr writer, must implement io.Writer
}

type RunParams struct {
	SessionID    string
	InvocationID string
	SkillName    string
	InputArgs    map[string]any
}

type SkillManager interface {
	Run(ctx context.Context, params *RunParams) (map[string]any, apperrors.Error)
}
