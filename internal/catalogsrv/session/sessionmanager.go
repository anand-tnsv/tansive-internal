package session

import (
	"context"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type SessionManager interface {
	Save(ctx context.Context) apperrors.Error
}
