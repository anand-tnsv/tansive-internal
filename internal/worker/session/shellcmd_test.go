package session

import (
	"context"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/mugiliam/goja_nodejs/console"
	"github.com/mugiliam/goja_nodejs/util"
	"github.com/stretchr/testify/require"
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

func TestShellCmdForGoja(t *testing.T) {
	vm := goja.New()
	util.SetGlobal(vm)
	console.SetGlobal(vm, nil)
	channel := NewChannel()
	channel.commandContext.shellConfig = &shellConfig{
		dir: "/tmp/test-shell-cmd",
	}
	InstallTansiveObject(context.Background(), vm, channel)
	script := `
	(function() {
 	 try {
    	tansive.shellCmd('javac ex.java', {timeout: 2000});
  	} catch (e) {
    	console.error("Error executing shell command:", e);
		console.error("Error signal:", e.signal);
		console.error("Error exit code:", e.exitCode);
		console.error("Error message:", e.message);
  	}
  	return "Command executed";
	})()
	`
	value, err := vm.RunString(script)
	if err != nil {
		t.Logf("shellCmdForGoja failed: %v", err)
		return
	}
	require.NoError(t, err)
	if value.String() != "Command executed" {
		t.Errorf("Expected 'Command executed', got '%s'", value.String())
	}

	script = `
	  tansive.shellCmd('ping 8.8.8.8', {timeout: 2000});
	  console.log("Command executed successfully");
	`
	value, err = vm.RunString(script)
	if err != nil {
		if jserr, ok := err.(*goja.Exception); ok {
			t.Logf("shellCmdForGoja failed: %s", jserr.Value().String())
		} else {
			t.Logf("shellCmdForGoja failed: %v", err)
		}
		return
	}
	require.Error(t, err)
	if value.String() != "Command executed successfully" {
		t.Errorf("Expected 'Command executed successfully', got '%s'", value.String())
	}
}

/*

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
)

// SpawnIsolatedShell sets up a pivot_root jail and launches /bin/bash inside it.
func SpawnIsolatedShell(sessionID string) error {
	jailRoot := "/tmp/jail-" + sessionID
	oldRoot := jailRoot + "/old_root"

	// Create minimal jail filesystem
	dirs := []string{"bin", "lib", "lib64", "usr", "proc", "old_root"}
	for _, dir := range dirs {
		if err := os.MkdirAll(jailRoot+"/"+dir, 0755); err != nil {
			return fmt.Errorf("mkdir %s failed: %w", dir, err)
		}
	}

	// Bind-mount needed directories
	bindMount("/bin", jailRoot+"/bin")
	bindMount("/lib", jailRoot+"/lib")
	bindMount("/lib64", jailRoot+"/lib64")
	bindMount("/usr", jailRoot+"/usr")
	mountProc(jailRoot + "/proc")
	bindMount(jailRoot, jailRoot) // make root a mount point

	// Unshare mount + pid namespaces
	if err := syscall.Unshare(syscall.CLONE_NEWNS | syscall.CLONE_NEWPID); err != nil {
		return fmt.Errorf("unshare failed: %w", err)
	}

	// Re-exec into new PID namespace to have init (PID 1)
	cmd := exec.Command("/proc/self/exe")
	cmd.Env = append(os.Environ(), "JAIL_ROOT="+jailRoot)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWPID,
	}
	return cmd.Run()
}

func init() {
	// Handle re-exec after unshare
	if jailRoot := os.Getenv("JAIL_ROOT"); jailRoot != "" {
		oldRoot := jailRoot + "/old_root"
		if err := syscall.PivotRoot(jailRoot, oldRoot); err != nil {
			log.Fatalf("pivot_root failed: %v", err)
		}
		if err := syscall.Chdir("/"); err != nil {
			log.Fatalf("chdir failed: %v", err)
		}
		_ = syscall.Unmount("/old_root", syscall.MNT_DETACH)
		_ = os.RemoveAll("/old_root")

		// Launch shell
		if err := syscall.Exec("/bin/bash", []string{"/bin/bash"}, os.Environ()); err != nil {
			log.Fatalf("exec failed: %v", err)
		}
	}
}

func bindMount(source, target string) {
	if err := syscall.Mount(source, target, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		log.Fatalf("bind mount %s -> %s failed: %v", source, target, err)
	}
}

func mountProc(target string) {
	if err := syscall.Mount("proc", target, "proc", 0, ""); err != nil {
		log.Fatalf("mount proc failed: %v", err)
	}
}

func main() {
	if err := SpawnIsolatedShell("abc123"); err != nil {
		log.Fatalf("failed to spawn isolated shell: %v", err)
	}
}

*/
