package session

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	src := `function test() {
		console.log("Hello, World!"); // This is a comment
		if (true) {
			console.log("Inside if block");
		} else {
			console.log("Inside else block");
		}
	}
	const x = 42; // Another comment`
	expected := []string{
		`function test() {`,
		`	console.log("Hello, World!"); // This is a comment`,
		`	if (true) {`,
		`		console.log("Inside if block");`,
		`	} else {`,
		`		console.log("Inside else block");`,
		`	}`,
		`}`}

	statements := TokenizeStatements(src)
	if len(statements) != len(expected) {
		t.Fatalf("Expected %d statements, got %d", len(expected), len(statements))
	}
	for i, stmt := range statements {
		if stmt != expected[i] {
			t.Errorf("Statement %d mismatch: expected %q, got %q", i, expected[i], stmt)
		}
	}
}

func TestTokenizeStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name: "Simple semicolon-separated statements",
			input: `
let x = 1;
let y = 2;
`,
			expected: []string{
				"let x = 1;",
				"let y = 2;",
			},
		},
		{
			name: "Newline-separated top-level statements",
			input: `
let x = 1
let y = 2
`,
			expected: []string{
				"let x = 1",
				"let y = 2",
			},
		},
		{
			name: "Multiline block with internal newlines",
			input: `
if (x > 0) {
  y = 2
  z = 3
}
console.log(x)
`,
			expected: []string{
				`if (x > 0) {
  y = 2
  z = 3
}`,
				"console.log(x)",
			},
		},
		{
			name: "Semicolon inside block, newline terminates outer",
			input: `
function f() {
  x = 1; y = 2;
}
f()
`,
			expected: []string{
				`function f() {
  x = 1; y = 2;
}`,
				"f()",
			},
		},
		{
			name: "String with semicolon inside",
			input: `
let str = "hello; world"
let x = 2
`,
			expected: []string{
				`let str = "hello; world"`,
				`let x = 2`,
			},
		},
		{
			name: "Quoted bracketed shell-like string",
			input: `
let s = "[ls -l]"
runShell("[already quoted]")
`,
			expected: []string{
				`let s = "[ls -l]"`,
				`runShell("[already quoted]")`,
			},
		},
		{
			name:  "Handles Windows CRLF line endings",
			input: "x = 1;\r\ny = 2;\r\n",
			expected: []string{
				"x = 1;",
				"y = 2;",
			},
		},
		{
			name: "Unclosed blocks don't break",
			input: `
if (true) {
  console.log("unclosed")
console.log("next")
`,
			expected: []string{
				`if (true) {
  console.log("unclosed")
console.log("next")`,
			},
		},
		{
			name:  "Handles escaped quotes",
			input: `let msg = "hello \"world\""; let x = 2`,
			expected: []string{
				`let msg = "hello \"world\"";`,
				`let x = 2`,
			},
		},
		{
			name: "Mishandled returns",
			input: `
			return
			42`,
			expected: []string{
				`return`,
				`42`,
			},
		},
		{
			name: "Try block with indented {}",
			input: `
			try {
				console.log("test") // safe function call
			} catch (e) {
				console.error(e)
			}
			return 42`,
			expected: []string{
				`try {
				console.log("test")
			} catch (e) {
				console.error(e)
			}`,
				`return 42`,
			},
		},
		{
			name: "Try block with non-indented {}",
			input: `
			try 
			{
				console.log("test") // safe function call
			} catch (e)
			{
				console.error(e) // log error
			}
			return 42`,
			expected: []string{
				`try {
				console.log("test")
			} catch (e) {
				console.error(e)
			}`,
				`return 42`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Try block with non-indented {}" {
				t.Logf("Running test: %s", tt.name)
			}
			got := TokenizeStatements(tt.input)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("\nExpected:\n%v\nGot:\n%v", tt.expected, got)
			}
		})
	}
}
