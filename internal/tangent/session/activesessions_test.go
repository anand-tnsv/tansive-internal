package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/runners/stdiorunner"
	"github.com/tansive/tansive-internal/internal/tangent/test"
)

func TestCreateSession(t *testing.T) {
	SetTestMode(true)
	stdiorunner.TestInit()
	ts := test.SetupTestCatalog(t)
	token, expiresAt := test.AdoptView(t, ts.Catalog, "dev-view", ts.Token)
	serverContext := &ServerContext{
		SessionID:      uuid.New(),
		TenantID:       ts.TenantID,
		Catalog:        ts.Catalog,
		Variant:        "dev",
		SkillSet:       test.SkillsetPath(),
		Skill:          test.SkillsetAgent(),
		View:           "dev-view",
		ViewDefinition: test.GetViewDefinition("dev"),
	}
	ctx := context.Background()
	session, err := ActiveSessionManager().CreateSession(ctx, serverContext, token, expiresAt)
	require.NoError(t, err)
	err = session.FetchObjects(ctx)
	require.NoError(t, err)
	tCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err = session.Run(tCtx, "k8s_troubleshooter", map[string]any{
		"prompt": "I'm getting a 500 error when I try to access the API",
	})
	require.NoError(t, err)
}
