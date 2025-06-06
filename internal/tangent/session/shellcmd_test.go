package session

import (
	"context"
	"testing"
	"time"
)

func TestShellCmd(t *testing.T) {
	cmd := "ping 8.8.8.8"
	sc := &shellConfig{
		dir: "/tmp/test-shell-cmd",
	}
	ctx, cancel := context.WithCancel(context.Background())
	// run a timer to invoke a cancel after 5 seconds
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := shellCmd(ctx, cmd, sc)
	if err == nil {
		t.Errorf("shellCmd failed: %v", err)
	}
}
