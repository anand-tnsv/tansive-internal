package jsruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		jsCode  string
		wantErr bool
	}{
		{
			name:    "valid function",
			jsCode:  "function(a, b) { return a + b; }",
			wantErr: false,
		},
		{
			name:    "valid arrow function",
			jsCode:  "(a, b) => a + b",
			wantErr: false,
		},
		{
			name:    "valid function with complex logic",
			jsCode:  "function(session, input) { return { sum: session.value + input.value, timestamp: Date.now() }; }",
			wantErr: false,
		},
		{
			name:    "invalid syntax",
			jsCode:  "function(a, b { return a + b; }", // missing closing parenthesis
			wantErr: true,
		},
		{
			name:    "not a function",
			jsCode:  "var x = 42;",
			wantErr: true,
		},
		{
			name:    "empty string",
			jsCode:  "",
			wantErr: true,
		},
		{
			name:    "just whitespace",
			jsCode:  "   \n\t  ",
			wantErr: true,
		},
		{
			name:    "function with console.log",
			jsCode:  "function(a, b) { console.log('test'); return a + b; }",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsFunc, err := New(tt.jsCode)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, jsFunc)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, jsFunc)
				assert.Equal(t, tt.jsCode, jsFunc.code)
				assert.NotNil(t, jsFunc.function)
			}
		})
	}
}

func TestJSFunction_Run(t *testing.T) {
	tests := []struct {
		name        string
		jsCode      string
		sessionArgs []byte
		inputArgs   []byte
		timeout     time.Duration
		wantResult  string
		wantErr     bool
	}{
		{
			name:        "simple addition",
			jsCode:      "function(a, b) { return a + b; }",
			sessionArgs: []byte(`5`),
			inputArgs:   []byte(`3`),
			wantResult:  `8`,
			wantErr:     false,
		},
		{
			name:        "object manipulation",
			jsCode:      "function(session, input) { return { result: session.value + input.value, session: session, input: input }; }",
			sessionArgs: []byte(`{"value": 10, "id": "session1"}`),
			inputArgs:   []byte(`{"value": 20, "id": "input1"}`),
			wantResult:  `{"input":{"id":"input1","value":20},"result":30,"session":{"id":"session1","value":10}}`,
			wantErr:     false,
		},
		{
			name:        "array operations",
			jsCode:      "function(session, input) { return session.items.concat(input.items); }",
			sessionArgs: []byte(`{"items": [1, 2, 3]}`),
			inputArgs:   []byte(`{"items": [4, 5, 6]}`),
			wantResult:  `[1,2,3,4,5,6]`,
			wantErr:     false,
		},
		{
			name:        "conditional logic",
			jsCode:      "function(session, input) { return session.enabled ? input.value : 0; }",
			sessionArgs: []byte(`{"enabled": true}`),
			inputArgs:   []byte(`{"value": 42}`),
			wantResult:  `42`,
			wantErr:     false,
		},
		{
			name:        "conditional logic false",
			jsCode:      "function(session, input) { return session.enabled ? input.value : 0; }",
			sessionArgs: []byte(`{"enabled": false}`),
			inputArgs:   []byte(`{"value": 42}`),
			wantResult:  `0`,
			wantErr:     false,
		},
		{
			name:        "null and undefined handling",
			jsCode:      "function(session, input) { return { sessionNull: session === null, inputUndefined: input === undefined, sessionType: typeof session, inputType: typeof input }; }",
			sessionArgs: []byte(`null`),
			inputArgs:   []byte(`null`),
			wantResult:  `{"inputType":"object","inputUndefined":false,"sessionNull":true,"sessionType":"object"}`,
			wantErr:     false,
		},
		{
			name:        "empty objects",
			jsCode:      "function(session, input) { return Object.keys(session).length + Object.keys(input).length; }",
			sessionArgs: []byte(`{}`),
			inputArgs:   []byte(`{}`),
			wantResult:  `0`,
			wantErr:     false,
		},
		{
			name:        "complex nested objects",
			jsCode:      "function(session, input) { return { deep: { nested: { value: session.config.deep.nested.value + input.config.deep.nested.value } } }; }",
			sessionArgs: []byte(`{"config": {"deep": {"nested": {"value": 100}}}}`),
			inputArgs:   []byte(`{"config": {"deep": {"nested": {"value": 200}}}}`),
			wantResult:  `{"deep":{"nested":{"value":300}}}`,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsFunc, err := New(tt.jsCode)
			require.NoError(t, err)

			opts := Options{Timeout: tt.timeout}
			if opts.Timeout == 0 {
				opts.Timeout = 100 * time.Millisecond
			}

			result, err := jsFunc.Run(context.Background(), tt.sessionArgs, tt.inputArgs, opts)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.JSONEq(t, tt.wantResult, string(result))
			}
		})
	}
}

func TestJSFunction_Run_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		jsCode      string
		sessionArgs []byte
		inputArgs   []byte
		timeout     time.Duration
		expectedErr error
	}{
		{
			name:        "invalid session args JSON",
			jsCode:      "function(a, b) { return a + b; }",
			sessionArgs: []byte(`{invalid json`),
			inputArgs:   []byte(`5`),
			expectedErr: ErrJSExecutionError,
		},
		{
			name:        "invalid input args JSON",
			jsCode:      "function(a, b) { return a + b; }",
			sessionArgs: []byte(`5`),
			inputArgs:   []byte(`{invalid json`),
			expectedErr: ErrJSExecutionError,
		},
		{
			name:        "runtime error in function",
			jsCode:      "function(a, b) { return a.nonExistentProperty.method(); }",
			sessionArgs: []byte(`{"value": 5}`),
			inputArgs:   []byte(`{"value": 3}`),
			expectedErr: ErrJSRuntimeError,
		},
		{
			name:        "reference error",
			jsCode:      "function(a, b) { return undefinedVariable; }",
			sessionArgs: []byte(`{"value": 5}`),
			inputArgs:   []byte(`{"value": 3}`),
			expectedErr: ErrJSRuntimeError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsFunc, err := New(tt.jsCode)
			require.NoError(t, err)

			opts := Options{Timeout: tt.timeout}
			if opts.Timeout == 0 {
				opts.Timeout = 100 * time.Millisecond
			}

			result, err := jsFunc.Run(context.Background(), tt.sessionArgs, tt.inputArgs, opts)
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.True(t, errors.Is(err, tt.expectedErr), "expected error to be %v, got %v", tt.expectedErr, err)
		})
	}
}

func TestJSFunction_Run_Timeout(t *testing.T) {
	// Function that runs indefinitely
	jsCode := "function(a, b) { while(true) { } return a + b; }"

	jsFunc, err := New(jsCode)
	require.NoError(t, err)

	opts := Options{Timeout: 10 * time.Millisecond}

	start := time.Now()
	result, err := jsFunc.Run(context.Background(), []byte(`5`), []byte(`3`), opts)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "timeout")
	assert.Less(t, duration, 50*time.Millisecond) // Should timeout quickly
}

func TestJSFunction_Run_DefaultTimeout(t *testing.T) {
	jsCode := "function(a, b) { return a + b; }"
	jsFunc, err := New(jsCode)
	require.NoError(t, err)

	// Test with zero timeout (should use default)
	opts := Options{Timeout: 0}

	result, err := jsFunc.Run(context.Background(), []byte(`5`), []byte(`3`), opts)
	assert.NoError(t, err)
	assert.Equal(t, `8`, string(result))
}

func TestJSFunction_Run_Isolation(t *testing.T) {
	// Test that each run uses a fresh VM instance
	jsCode := "function(a, b) { if (!a.counter) a.counter = 0; a.counter++; return { counter: a.counter, sum: a + b }; }"

	jsFunc, err := New(jsCode)
	require.NoError(t, err)

	opts := Options{Timeout: 100 * time.Millisecond}

	// First run
	firstSession := map[string]interface{}{}
	firstSessionBytes, _ := json.Marshal(firstSession)
	result1, err := jsFunc.Run(context.Background(), firstSessionBytes, []byte(`3`), opts)
	require.NoError(t, err)

	var result1Obj map[string]interface{}
	if err := json.Unmarshal(result1, &result1Obj); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, float64(1), result1Obj["counter"])

	// Second run - should start fresh
	secondSession := map[string]interface{}{}
	secondSessionBytes, _ := json.Marshal(secondSession)
	result2, err := jsFunc.Run(context.Background(), secondSessionBytes, []byte(`20`), opts)
	require.NoError(t, err)

	var result2Obj map[string]interface{}
	if err := json.Unmarshal(result2, &result2Obj); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, float64(1), result2Obj["counter"]) // Should be 1 again, not 2
}

func TestJSFunction_Run_ConsoleLog(t *testing.T) {
	jsCode := "function(a, b) { console.log('Session:', a, 'Input:', b); return a + b; }"

	jsFunc, err := New(jsCode)
	require.NoError(t, err)

	opts := Options{Timeout: 100 * time.Millisecond}

	// This should not panic and should execute successfully
	result, err := jsFunc.Run(context.Background(), []byte(`5`), []byte(`3`), opts)
	assert.NoError(t, err)
	assert.Equal(t, `8`, string(result))
}

func TestJSFunction_Run_LargeData(t *testing.T) {
	// Test with large JSON objects
	largeSession := make(map[string]interface{})
	largeInput := make(map[string]interface{})

	for i := 0; i < 1000; i++ {
		largeSession[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
		largeInput[fmt.Sprintf("inputKey%d", i)] = i
	}

	sessionBytes, err := json.Marshal(largeSession)
	require.NoError(t, err)

	inputBytes, err := json.Marshal(largeInput)
	require.NoError(t, err)

	jsCode := "function(session, input) { return { sessionKeys: Object.keys(session).length, inputKeys: Object.keys(input).length, sum: Object.keys(session).length + Object.keys(input).length }; }"

	jsFunc, err := New(jsCode)
	require.NoError(t, err)

	opts := Options{Timeout: 1 * time.Second}

	result, err := jsFunc.Run(context.Background(), sessionBytes, inputBytes, opts)
	assert.NoError(t, err)

	var resultObj map[string]interface{}
	err = json.Unmarshal(result, &resultObj)
	require.NoError(t, err)
	assert.Equal(t, float64(1000), resultObj["sessionKeys"])
	assert.Equal(t, float64(1000), resultObj["inputKeys"])
	assert.Equal(t, float64(2000), resultObj["sum"])
}

func TestJSFunction_Run_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		jsCode      string
		sessionArgs []byte
		inputArgs   []byte
		expected    string
	}{
		{
			name:        "empty arrays",
			jsCode:      "function(a, b) { return [a.length, b.length]; }",
			sessionArgs: []byte(`[]`),
			inputArgs:   []byte(`[]`),
			expected:    `[0,0]`,
		},
		{
			name:        "null values",
			jsCode:      "function(a, b) { return { aNull: a === null, bNull: b === null, aType: typeof a, bType: typeof b }; }",
			sessionArgs: []byte(`null`),
			inputArgs:   []byte(`null`),
			expected:    `{"aNull":true,"aType":"object","bNull":true,"bType":"object"}`,
		},
		{
			name:        "boolean values",
			jsCode:      "function(a, b) { return { aBool: typeof a, bBool: typeof b, result: a && b }; }",
			sessionArgs: []byte(`true`),
			inputArgs:   []byte(`false`),
			expected:    `{"aBool":"boolean","bBool":"boolean","result":false}`,
		},
		{
			name:        "string values",
			jsCode:      "function(a, b) { return a + ' ' + b; }",
			sessionArgs: []byte(`"hello"`),
			inputArgs:   []byte(`"world"`),
			expected:    `"hello world"`,
		},
		{
			name:        "number values",
			jsCode:      "function(a, b) { return { sum: a + b, product: a * b, quotient: a / b }; }",
			sessionArgs: []byte(`10`),
			inputArgs:   []byte(`5`),
			expected:    `{"product":50,"quotient":2,"sum":15}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsFunc, err := New(tt.jsCode)
			require.NoError(t, err)

			opts := Options{Timeout: 100 * time.Millisecond}

			result, err := jsFunc.Run(context.Background(), tt.sessionArgs, tt.inputArgs, opts)
			assert.NoError(t, err)
			assert.JSONEq(t, tt.expected, string(result))
		})
	}
}

func TestJSFunction_Run_PanicRecovery(t *testing.T) {
	// Test that panics in the JavaScript code are properly recovered
	jsCode := "function(a, b) { throw new Error('Test error'); }"

	jsFunc, err := New(jsCode)
	require.NoError(t, err)

	opts := Options{Timeout: 100 * time.Millisecond}

	result, err := jsFunc.Run(context.Background(), []byte(`5`), []byte(`3`), opts)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrJSRuntimeError)
	assert.Contains(t, err.Error(), "Test error")
}

func BenchmarkJSFunction_Run(b *testing.B) {
	jsCode := "function(session, input) { return { result: session.value + input.value, timestamp: Date.now() }; }"
	jsFunc, err := New(jsCode)
	require.NoError(b, err)

	sessionArgs := []byte(`{"value": 10, "id": "session1"}`)
	inputArgs := []byte(`{"value": 20, "id": "input1"}`)
	opts := Options{Timeout: 100 * time.Millisecond}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := jsFunc.Run(context.Background(), sessionArgs, inputArgs, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSFunction_New(b *testing.B) {
	jsCode := "function(session, input) { return session.value + input.value; }"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := New(jsCode)
		if err != nil {
			b.Fatal(err)
		}
	}
}
