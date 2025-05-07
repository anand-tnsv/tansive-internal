package session

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dop251/goja"
)

type tansivejs struct {
	vm      *goja.Runtime
	channel *channel
}

func (t *tansivejs) jsShellCmd(ctx context.Context) func(call goja.FunctionCall) goja.Value {
	return func(call goja.FunctionCall) goja.Value {
		var stdout, stderr bytes.Buffer
		argLen := len(call.Arguments)
		if argLen == 0 {
			panic(t.vm.NewTypeError("shellCmd: at least one argument is required"))
		}

		var timeout int64
		var cmdArgs []string

		// Check if last argument is an options object
		lastArg := call.Arguments[argLen-1]
		if obj := lastArg.ToObject(t.vm); obj != nil && obj.ClassName() == "Object" {
			// Treat as options object
			timeoutVal := obj.Get("timeout")
			if timeoutVal != nil && !goja.IsUndefined(timeoutVal) && !goja.IsNull(timeoutVal) {
				timeout = timeoutVal.ToInteger()
			}
			// Everything before the last argument is command args
			for _, arg := range call.Arguments[:argLen-1] {
				cmdArgs = append(cmdArgs, arg.String())
			}
		} else {
			// No options object, treat all args as command parts
			for _, arg := range call.Arguments {
				cmdArgs = append(cmdArgs, arg.String())
			}
		}

		if len(cmdArgs) == 0 {
			panic(t.vm.NewTypeError("shellCmd: command string is empty"))
		}

		command := strings.Join(cmdArgs, " ")

		ctxCmd, cancel := context.WithCancel(ctx)
		defer cancel()

		// Start timeout watcher if needed
		isTimedOut := false
		go func() {
			defer func() {
				fmt.Println("Command execution completed")
			}()
			for {
				select {
				case <-ctx.Done():
					// If the context is done, cancel the command
					cancel()
				case <-time.After(time.Duration(timeout) * time.Millisecond):
					// If the interval has passed, cancel the command
					if timeout > 0 {
						isTimedOut = true
						cancel()
					}
				case <-ctxCmd.Done():
					// If the command context is done, return
					return
				}
			}
		}()

		cmd, err := createCommand(ctxCmd, command, &commandIOWriters{
			out: &stdout,
			err: &stderr,
		}, t.channel.commandContext.shellConfig)
		if err != nil {
			panic(t.vm.NewTypeError("%s\n%s", stderr.String(), err.Error()))
		}

		if err := cmd.Run(); err != nil {
			b := strings.Builder{}
			b.WriteString(stderr.String())
			if b.Len() > 0 {
				b.WriteString(": ")
			}
			if isTimedOut {
				b.WriteString(fmt.Sprintf("command timed out after %d milliseconds", timeout))
				b.WriteString(": ")
			}
			b.WriteString(err.Error())
			errInfo := extractCommandError(err, stderr.String())
			panic(t.jsErrorObject(b.String(), errInfo.ExitCode, errInfo.Stderr, errInfo.Signal))
		}

		return t.vm.ToValue(stdout.String())
	}
}

func (t *tansivejs) jsErrorObject(message string, exitCode int, stderr string, signal string) *goja.Object {
	// Create a real JS Error instance
	val, err := t.vm.RunString(fmt.Sprintf("new Error(%q)", message))
	if err != nil {
		panic("failed to create JS Error: " + err.Error())
	}

	errObj := val.ToObject(t.vm)

	// Attach additional fields
	_ = errObj.Set("exitCode", exitCode)
	_ = errObj.Set("stderr", stderr)
	if signal != "" {
		_ = errObj.Set("signal", signal)
	}

	return errObj
}

func InstallTansiveObject(ctx context.Context, vm *goja.Runtime, channel *channel) {
	t := &tansivejs{
		vm:      vm,
		channel: channel,
	}

	obj := vm.NewObject()
	obj.Set("shellCmd", t.jsShellCmd(ctx))
	vm.Set("tansive", obj)
}
