package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/common/jsonrpc"
)

func TestCommandRequest(t *testing.T) {
	req := &RunCommandRequest{
		SessionId: uuid.New(),
		Data:      "ping 8.8.8",
		Kernel:    KernelTypeShell,
	}
	jreq, err := jsonrpc.ConstructRequest(
		NewMarker(),
		MethodRunCommand,
		req,
	)
	require.NoError(t, err)
	msg, err := jsonrpc.ParseRequest(jreq)
	require.NoError(t, err)

	w := NewStdoutMessageWriter()
	c := &commandContext{
		mu: &sync.Mutex{},
		shellConfig: &shellConfig{
			dir: "/tmp/test-shell-cmd",
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		apperr := HandleRunCommandRequest(ctx, msg, w, c)
		t.Logf("completed command")
		require.NoError(t, apperr)
	}()
	<-ctx.Done()
	time.Sleep(1 * time.Second)
}
