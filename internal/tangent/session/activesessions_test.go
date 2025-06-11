package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/runners/stdiorunner"
	"github.com/tansive/tansive-internal/internal/tangent/session/skillservice"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
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
	err = session.fetchObjects(ctx)
	require.NoError(t, err)
	tCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	outWriter := tangentcommon.NewBufferedWriter()
	errWriter := tangentcommon.NewBufferedWriter()
	err = session.Run(tCtx, "", "k8s_troubleshooter", map[string]any{
		"prompt": "I'm getting a 500 error when I try to access the API",
	}, &tangentcommon.IOWriters{
		Out: outWriter,
		Err: errWriter,
	})
	require.NoError(t, err)
	t.Logf("outWriter: %s", outWriter.String())
	t.Logf("errWriter: %s", errWriter.String())
	CreateSkillService()
	// wait for few milliseconds to ensure the skill service is running
	time.Sleep(100 * time.Millisecond)
	client, goerr := skillservice.NewClient()
	require.NoError(t, goerr)
	defer client.Close()

	// empty invocationID should return an error
	_, goerr = client.InvokeSkill(ctx, session.GetSessionID(), "", "k8s_troubleshooter", map[string]any{
		"prompt": "I'm getting a 500 error when I try to access the API",
	})
	require.Error(t, goerr)

	// reusing invocationID should return a loop error
	var invocationID string
	for k := range session.invocationIDs {
		invocationID = k
		break
	}
	_, goerr = client.InvokeSkill(ctx, session.GetSessionID(), invocationID, "k8s_troubleshooter", map[string]any{
		"prompt": "I'm getting a 500 error when I try to access the API",
	})
	require.Error(t, goerr)

	// for testing, append a new invocationID to the session
	invocationID = uuid.New().String()
	session.invocationIDs[invocationID] = session.viewDef
	response, goerr := client.InvokeSkill(ctx, session.GetSessionID(), invocationID, "k8s_troubleshooter", map[string]any{
		"prompt": "I'm getting a 500 error when I try to access the API",
	})
	if !assert.NoError(t, goerr) {
		t.Logf("error: %v", goerr.Error())
		if response != nil {
			t.Logf("response: %v", response)
		}
		t.Fail()
	}
	// Extract output from the protobuf struct
	if response != nil {
		t.Logf("response output: %s", response.Output["content"])
	}

	// test get tools
	tools, goerr := client.GetTools(ctx, session.GetSessionID())
	require.NoError(t, goerr)
	require.NotNil(t, tools)
	t.Logf("tools: %v", tools)
}
