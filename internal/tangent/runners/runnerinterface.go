package runners

import (
	"context"
	"fmt"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/tangent/runners/stdiorunner"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
	"github.com/tansive/tansive-internal/pkg/api"
)

// Runner is the interface for all runners.
type Runner interface {
	ID() string
	AddWriters(writers ...*tangentcommon.IOWriters)
	Run(ctx context.Context, args *api.SkillInputArgs) apperrors.Error
}

func NewRunner(ctx context.Context, sessionID string, runnerDef catalogmanager.SkillSetSource, writers ...*tangentcommon.IOWriters) (Runner, apperrors.Error) {
	switch runnerDef.Runner {
	case catcommon.StdioRunnerID:
		return stdiorunner.New(ctx, sessionID, runnerDef.Config, writers...)
	default:
		return nil, apperrors.New(fmt.Sprintf("invalid runner id: %s", runnerDef.Runner))
	}
}

func Init() {
	stdiorunner.Init()
}
