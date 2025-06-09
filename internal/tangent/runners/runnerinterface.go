package runners

import (
	"context"
	"encoding/json"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

// Runner is the interface for all runners.
type Runner interface {
	Run(ctx context.Context, args json.RawMessage) apperrors.Error
}
