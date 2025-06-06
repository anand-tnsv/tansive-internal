package session

import (
	"testing"

	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/tansive/tansive-internal/internal/tangent/test"
)

func TestCreateSession(t *testing.T) {
	ts := test.SetupTestCatalog(t)
	token := test.AdoptView(t, ts.Catalog, "dev-view", ts.Token)
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
}
