package jsruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dop251/goja"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

type JSFunction struct {
	code     string
	function goja.Callable
}

// Options for controlling execution
type Options struct {
	Timeout time.Duration // max execution time
}

// New creates a JSFunction from a JS function source string.
func New(jsCode string) (*JSFunction, apperrors.Error) {
	vm := goja.New()
	bindConsole(vm)

	wrapped := fmt.Sprintf("(%s)", jsCode)
	v, err := vm.RunString(wrapped)
	if err != nil {
		return nil, ErrInvalidJSFunction.Err(err)
	}

	fn, ok := goja.AssertFunction(v)
	if !ok {
		return nil, ErrInvalidJSFunction.Msg("script is not a function")
	}

	return &JSFunction{
		code:     jsCode,
		function: fn,
	}, nil
}

// Run executes the function with two JSON arguments, respecting timeout and returning JSON output.
func (j *JSFunction) Run(ctx context.Context, sessionArgs, inputArgs []byte, opts Options) ([]byte, apperrors.Error) {
	// New VM per run to isolate memory
	vm := goja.New()
	bindConsole(vm)

	// recompile function
	wrapped := fmt.Sprintf("(%s)", j.code)
	v, err := vm.RunString(wrapped)
	if err != nil {
		return nil, ErrJSExecutionError.Err(err)
	}
	fn, _ := goja.AssertFunction(v)

	// Parse input
	var obj1, obj2 any
	if err := json.Unmarshal(sessionArgs, &obj1); err != nil {
		return nil, ErrJSExecutionError.Msg("invalid session args").Err(err)
	}
	if err := json.Unmarshal(inputArgs, &obj2); err != nil {
		return nil, ErrJSExecutionError.Msg("invalid input args").Err(err)
	}

	// Use context with timeout
	if opts.Timeout == 0 {
		opts.Timeout = 500 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	done := make(chan struct{})
	var result goja.Value
	var callErr error

	go func() {
		defer func() {
			if r := recover(); r != nil {
				callErr = fmt.Errorf("panic: %v", r)
			}
			close(done)
		}()

		val1 := vm.ToValue(obj1)
		val2 := vm.ToValue(obj2)
		result, callErr = fn(goja.Undefined(), val1, val2)
	}()

	select {
	case <-ctx.Done():
		return nil, ErrJSRuntimeTimeout
	case <-done:
		if callErr != nil {
			if jsErr, ok := callErr.(*goja.Exception); ok {
				return nil, ErrJSRuntimeError.Msg(jsErr.Value().String())
			}
			return nil, ErrJSExecutionError.Err(callErr)
		}
	}

	// Convert back to JSON
	exported := result.Export()
	jsonBytes, err := json.Marshal(exported)
	if err != nil {
		return nil, ErrJSExecutionError.Msg("failed to marshal result").Err(err)
	}
	return jsonBytes, nil
}
