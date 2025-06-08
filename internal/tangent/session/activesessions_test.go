package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/test"
)

func TestCreateSession(t *testing.T) {
	SetTestMode(true)
	ts := test.SetupTestCatalog(t)
	token, expiresAt := test.AdoptView(t, ts.Catalog, "dev-view", ts.Token)
	t.Logf("Token: %s", token)
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
	err = session.RunInteractiveSkill(tCtx)
	require.NoError(t, err)
}
