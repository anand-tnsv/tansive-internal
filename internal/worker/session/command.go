package session

import (
	"regexp"
	"strings"
)

// shellPattern matches bracketed expressions that don't contain quotes, commas, or nested structures.
var shellPattern = regexp.MustCompile(`^\s*([^\[\]\{\},'"]+?)\s*$`)

// PreprocessJavaScript scans JS code and replaces [cmd args] with runShell("cmd args")
// Only when outside of string literals and the bracketed part is safe to replace.
func PreprocessJavaScript(js string) string {
	statements := TokenizeStatements(js)
	for i, stmt := range statements {
		statements[i] = substituteShellCalls(stmt)
	}
	return strings.Join(statements, "\n")
}

// TokenizeStatements splits JavaScript source into complete statements using semicolon boundaries
// and accounting for string literals and bracket balance.
func TokenizeStatements(src string) []string {
	var statements []string
	var current strings.Builder
	var stack []rune
	inString := false
	var quote rune
	src = strings.ReplaceAll(src, "\r\n", "\n") // Windows
	src = strings.ReplaceAll(src, "\r", "\n")   // Old Mac
	s := []rune(src)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		current.WriteRune(ch)

		if inString {
			if ch == quote {
				// Count how many backslashes before quote
				bs := 0
				for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
					bs++
				}
				if bs%2 == 0 { // even number of backslashes => not escaped
					inString = false
				}
			}
			continue
		}

		if ch == '"' || ch == '\'' {
			inString = true
			quote = ch
			continue
		}

		switch ch {
		case '{', '(', '[':
			stack = append(stack, ch)
		case '}', ')', ']':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case '\n', ';':
			// If the bracket stack is empty, treat newline or semicolon as statement separator
			if len(stack) == 0 {
				stmt := strings.TrimSpace(current.String())
				if stmt != "" {
					statements = append(statements, stmt)
				}
				current.Reset()
			}
		}
	}

	if current.Len() > 0 {
		statements = append(statements, strings.TrimSpace(current.String()))
	}

	return statements
}

// substituteShellCalls replaces unquoted bracketed expressions with runShell(...)
func substituteShellCalls(js string) string {
	var out strings.Builder
	inString := false
	var quote rune
	src := []rune(js)
	i := 0
	for i < len(src) {
		c := src[i]

		if inString {
			out.WriteRune(c)
			if c == quote && (i == 0 || src[i-1] != '\\') {
				inString = false
			}
			i++
			continue
		}

		if c == '"' || c == '\'' {
			inString = true
			quote = c
			out.WriteRune(c)
			i++
			continue
		}

		if c == '[' {
			start := i
			depth := 1
			i++
			for i < len(src) && depth > 0 {
				if src[i] == '[' {
					depth++
				} else if src[i] == ']' {
					depth--
				}
				i++
			}
			if depth == 0 {
				segment := string(src[start:i])
				inner := strings.TrimSpace(segment[1 : len(segment)-1])
				if shellPattern.MatchString(inner) {
					out.WriteString(`runShell("` + inner + `")`)
				} else {
					out.WriteString(segment)
				}
				continue
			} else {
				// malformed bracket expression
				out.WriteRune(c)
			}
		} else {
			out.WriteRune(c)
			i++
		}
	}

	return out.String()
}
