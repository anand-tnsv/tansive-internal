package session

import (
	"errors"
	"slices"
	"strings"
)

type TokenType string

const (
	TokenTypeDocument TokenType = "document"
	TokenTypeCmd      TokenType = "cmd"
	TokenTypeString   TokenType = "string"
	TokenTypeShell    TokenType = "shell"
	TokenTypeVar      TokenType = "var"
)

type Token struct {
	Type     TokenType
	Value    string
	Children []*Token
	Parent   *Token
}

func (t *Token) Add(child *Token) {
	child.Parent = t
	t.Children = append(t.Children, child)
}

func isCmd(cmd string) bool {
	return slices.Contains([]string{"get", "put"}, cmd)
}

func isLineCmd(cmd string) bool {
	return slices.Contains([]string{"sh"}, cmd)
}

func parseLine(line string) (*Token, error) {
	delims := map[rune]int{}
	runes := []rune(line)
	length := len(runes)
	pos := 0
	atLineStart := true
	var argBuilder strings.Builder

	root := &Token{Type: TokenTypeDocument}
	current := root

	for pos < length {
		r := runes[pos]

		if pos != 0 && r != '\n' && atLineStart {
			atLineStart = false
		}

		switch r {
		case '@':
			if isEscaped(runes, pos) {
				removeLastRune(&argBuilder)
				argBuilder.WriteRune(r)
				break
			}
			if atLineStart {
				cmd, end, term := extractUntil(runes, pos+1, ' ')
				if term == 0 {
					argBuilder.WriteRune(r)
					break
				}
				if isLineCmd(cmd) {
					if delims['\n'] > 0 {
						return nil, errors.New("invalid command")
					}
					delims['\n']++
					pos = end
					current = appendToken(current, &argBuilder, &Token{Type: TokenTypeShell, Value: cmd})
					break
				}
			}

			cmd, end, term := extractUntil(runes, pos+1, '(')
			if term != 0 && isCmd(cmd) {
				pos = end
				delims['(']++
				current = appendToken(current, &argBuilder, &Token{Type: TokenTypeCmd, Value: strings.TrimSpace(cmd)})
			} else if pos+1 < length && runes[pos+1] == '{' {
				pos++
				delims['{']++
				current = appendToken(current, &argBuilder, &Token{Type: TokenTypeVar})
			} else {
				argBuilder.WriteRune(r)
			}

		case ')':
			if isEscaped(runes, pos) {
				removeLastRune(&argBuilder)
				argBuilder.WriteRune(r)
				break
			}
			if delims['('] > 0 {
				delims['(']--
				current = closeContext(current, &argBuilder)
			} else {
				argBuilder.WriteRune(r)
			}

		case '}':
			if isEscaped(runes, pos) {
				removeLastRune(&argBuilder)
				argBuilder.WriteRune(r)
				break
			}
			if delims['{'] > 0 {
				delims['{']--
				current = closeContext(current, &argBuilder)
			} else {
				argBuilder.WriteRune(r)
			}

		case '"':
			if isEscaped(runes, pos) {
				removeLastRune(&argBuilder)
			}
			delims['"'] ^= 1
			argBuilder.WriteRune(r)

		case ',':
			if isEscaped(runes, pos) {
				argBuilder.WriteRune(r)
				break
			}
			if delims['('] > 0 && delims['"'] == 0 {
				current = appendToken(current, &argBuilder, &Token{Type: TokenTypeString, Value: argBuilder.String()})
				argBuilder.Reset()
			}
			argBuilder.WriteRune(r)

		case '\n':
			if isEscaped(runes, pos) {
				argBuilder.WriteRune(r)
				break
			}
			if delims['\n'] > 0 {
				delims['\n']--
				current = closeContext(current, &argBuilder)
			}
			argBuilder.WriteRune(r)
			atLineStart = true

		default:
			argBuilder.WriteRune(r)
		}
		pos++
	}

	if argBuilder.Len() > 0 {
		current = appendToken(current, &argBuilder, &Token{Type: TokenTypeString, Value: argBuilder.String()})
		argBuilder.Reset()
	}

	for current.Type != TokenTypeDocument && current.Parent != nil {
		current = current.Parent
	}

	if current.Type != TokenTypeDocument {
		return nil, errors.New("incomplete expression")
	}
	return root, nil
}

func appendToken(current *Token, b *strings.Builder, token *Token) *Token {
	if b.Len() > 0 {
		current.Add(&Token{Type: TokenTypeString, Value: b.String()})
		b.Reset()
	}
	current.Add(token)
	return token
}

func closeContext(current *Token, b *strings.Builder) *Token {
	if b.Len() > 0 {
		current.Add(&Token{Type: TokenTypeString, Value: b.String()})
		b.Reset()
	}
	if current.Parent != nil {
		return current.Parent
	}
	return current
}

func extractUntil(runes []rune, start int, term rune) (string, int, rune) {
	for i := start; i < len(runes); i++ {
		if runes[i] == term {
			return string(runes[start:i]), i, term
		}
	}
	return string(runes[start:]), len(runes), 0
}

func isEscaped(runes []rune, pos int) bool {
	count := 0
	for i := pos - 1; i >= 0 && runes[i] == '\\'; i-- {
		count++
	}
	return count%2 != 0
}

func removeLastRune(b *strings.Builder) {
	s := b.String()
	r := []rune(s)
	if len(r) > 0 {
		b.Reset()
		b.WriteString(string(r[:len(r)-1]))
	}
}
