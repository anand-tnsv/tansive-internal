package skillservice

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/tangent/tangentcommon"
)

// mockSession implements tangentcommon.Session interface for testing
type mockSession struct {
	id string
}

func (m *mockSession) GetSessionID() string {
	return m.id
}

func (m *mockSession) Run(ctx context.Context, params *tangentcommon.RunParams) (map[string]any, apperrors.Error) {
	return nil, nil
}

func TestSkillService(t *testing.T) {
	// Create and start the server
	server, err := NewServer()
	require.NoError(t, err)

	// Create and register the skill service
	skillService := NewSkillService(&mockSession{id: "test-session"})
	server.RegisterService(skillService)

	t.Cleanup(func() {
		server.Stop()
	})

	// Start server in a goroutine
	go func() {
		err := server.Start()
		require.NoError(t, err)
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create a client
	client, err := NewClient()
	require.NoError(t, err)
	defer client.Close()

	// Test cases
	t.Run("InvokeSkill", func(t *testing.T) {
		ctx := context.Background()
		sessionID := "test-session"
		skillName := "test-skill"
		args := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}

		result, err := client.InvokeSkill(ctx, "some-invocation-id", sessionID, skillName, args)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotEmpty(t, result.InvocationId)
		require.NotNil(t, result.Output)
		//	require.Equal(t, "success", result.Output.Fields["status"].GetStringValue())
		//	require.Contains(t, result.Output.Fields["message"].GetStringValue(), skillName)
	})
}

func TestServerStartStop(t *testing.T) {
	server, err := NewServer()
	require.NoError(t, err)

	// Get the socket path before starting the server
	socketPath, err := GetSocketPath()
	require.NoError(t, err)

	// Start server in a goroutine
	go func() {
		err := server.Start()
		require.NoError(t, err)
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Verify socket file exists
	_, err = os.Stat(socketPath)
	require.NoError(t, err, "socket file should exist after server start")

	// Stop the server
	server.Stop()

	// Verify socket file is cleaned up
	_, err = os.Stat(socketPath)
	require.Error(t, err, "socket file should be removed after server stop")
	require.True(t, os.IsNotExist(err), "error should be 'file does not exist'")
}

func TestGetSocketPath(t *testing.T) {
	path, err := GetSocketPath()
	require.NoError(t, err)
	require.NotEmpty(t, path)
	require.Contains(t, path, defaultSocketName)
}
