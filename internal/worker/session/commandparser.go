package session

import (
	"errors"
	"slices"
	"strings"
)

type TokenType string

const (
	TokenTypeNone   = "none"
	TokenTypeCmd    = "cmd"
	TokenTypeString = "string"
	TokenTypeShell  = "shell"
	TokenTypeVar    = "var"
)

type Token struct {
	Type     TokenType
	Value    string
	children []*Token
	parent   *Token
}

func isCmd(cmd string) bool {
	validCommands := []string{
		"get",
		"put",
	}
	return slices.Contains(validCommands, cmd)
}

func isLineCmd(cmd string) bool {
	validCommands := []string{
		"sh",
	}
	for _, validCmd := range validCommands {
		if cmd == validCmd {
			return true
		}
	}
	return false
}
func parseLine(line string) (*Token, error) {
	var delimiterState = make(map[rune]int)
	var rootToken *Token = nil
	var lastToken *Token = nil
	runes := []rune(line)
	len := len(runes)
	pos := 0
	columnZero := true
	var argString strings.Builder
	for pos < len {
		if pos != 0 && runes[pos] != '\n' && columnZero {
			columnZero = false
		}
		switch runes[pos] {
		case '@':
			if isPositionEscaped(runes, pos) {
				removeLastRune(&argString)
				argString.WriteRune(runes[pos])
				break
			}
			if columnZero {
				cmd, end, term := extractTokenUntilTerminators(runes, pos+1, []rune{' ', '('})
				if term == 0 {
					argString.WriteRune(runes[pos])
					break
				}
				if isLineCmd(cmd) {
					if delimiterState['\n'] > 0 {
						return nil, errors.New("invalid command")
					}
					delimiterState['\n']++
					pos = end
					lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeShell, Value: cmd}, &argString)
					break
				}
			}
			cmd, end, term := extractTokenUntilTerminators(runes, pos+1, []rune{'('})
			if term != 0 && isCmd(cmd) {
				pos = end
				delimiterState['(']++
				lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeCmd, Value: strings.Trim(cmd, " ")}, &argString)
			} else if pos+1 < len && runes[pos+1] == '{' && delimiterState['"'] == 0 {
				pos++
				delimiterState['{']++
				lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeVar}, &argString)
			} else {
				argString.WriteRune(runes[pos])
			}

		case ')':
			if isPositionEscaped(runes, pos) {
				removeLastRune(&argString)
				argString.WriteRune(runes[pos])
				break
			}
			if delimiterState['('] > 0 {
				delimiterState['(']--
				if delimiterState['('] == 0 {
					if argString.Len() > 0 {
						str := argString.String()
						argString.Reset()
						lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeString, Value: str}, &argString)
						lastToken = lastToken.parent.parent
					} else {
						lastToken = lastToken.parent
					}
				}
			} else {
				argString.WriteRune(runes[pos])
			}

		case '{':
			if isPositionEscaped(runes, pos) {
				removeLastRune(&argString)
				argString.WriteRune(runes[pos])
				break
			}
			if delimiterState['"'] > 0 {
				delimiterState['{']++
				lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeVar}, &argString)
			} else {
				argString.WriteRune(runes[pos])
			}

		case '}':
			if isPositionEscaped(runes, pos) {
				removeLastRune(&argString)
				argString.WriteRune(runes[pos])
				break
			}
			if delimiterState['{'] > 0 {
				delimiterState['{']--
				if delimiterState['{'] == 0 {
					if argString.Len() > 0 {
						str := argString.String()
						argString.Reset()
						lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeString, Value: str}, &argString)
						lastToken = lastToken.parent.parent
					} else {
						lastToken = lastToken.parent
					}
				}
			} else {
				// if we are not in a var, then just add the token
				argString.WriteRune(runes[pos])
			}

		case '"':
			if isPositionEscaped(runes, pos) {
				removeLastRune(&argString)
			}
			// if delimiterState is odd, then subtract 1
			if delimiterState['"']%2 != 0 {
				delimiterState['"']--
			} else {
				delimiterState['"']++
			}
			argString.WriteRune(runes[pos])

		case ',':
			if isPositionEscaped(runes, pos) {
				argString.WriteRune(runes[pos])
			}
			if delimiterState['('] > 0 && delimiterState['"']%2 != 0 {
				if argString.Len() > 0 {
					str := argString.String()
					argString.Reset()
					lastToken = addToken(&rootToken, lastToken, &Token{Type: TokenTypeString, Value: str}, &argString)
				}
			}
			argString.WriteRune(runes[pos])

		case '\n':
			if isPositionEscaped(runes, pos) {
				argString.WriteRune(runes[pos])
				break
			}
			columnZero = true

		default:
			argString.WriteRune(runes[pos])
		}
		pos++
		continue
	}

	if argString.Len() > 0 {
		str := argString.String()
		argString.Reset()
		addToken(&rootToken, lastToken, &Token{Type: TokenTypeString, Value: str}, &argString)
	}
	if lastToken != nil {
		lastToken = lastToken.parent
	}
	if lastToken != nil {
		return nil, errors.New("incomplete expression")
	}
	return rootToken, nil
}

func addToken(rootToken **Token, lastToken *Token, token *Token, b *strings.Builder) *Token {
	if lastToken == nil {
		*rootToken = token
		token.parent = nil
		return token
	} else if lastToken.Type == TokenTypeCmd {
		if b.Len() > 0 {
			lastToken.children = append(lastToken.children, &Token{Type: TokenTypeString, Value: b.String()})
			b.Reset()
		}
		lastToken.children = append(lastToken.children, token)
		token.parent = lastToken
	} else if lastToken.Type == TokenTypeShell {
		if b.Len() > 0 {
			lastToken.children = append(lastToken.children, &Token{Type: TokenTypeString, Value: b.String()})
			b.Reset()
		}
		lastToken.children = append(lastToken.children, token)
		token.parent = lastToken
	} else if lastToken.Type == TokenTypeString {
		if token.Type == TokenTypeString {
			lastToken.parent.children = append(lastToken.parent.children, token)
			token.parent = lastToken.parent
		} else if token.Type == TokenTypeVar {
			if b.Len() > 0 {
				lastToken.children = append(lastToken.children, &Token{Type: TokenTypeString, Value: b.String()})
				b.Reset()
			}
			lastToken.children = append(lastToken.children, token)
			token.parent = lastToken
		}
	} else if lastToken.Type == TokenTypeVar {
		if b.Len() > 0 {
			lastToken.children = append(lastToken.children, &Token{Type: TokenTypeString, Value: b.String()})
			b.Reset()
		}
		lastToken.children = append(lastToken.children, token)
		token.parent = lastToken
	}
	return token
}

/*
@sh echo "Hello, World!"
@sh echo "{hello}"
@sh "echo {hello}"
@sh "echo \"{hello}\""
@sh ("echo {hello}", @{r}, {"hello": "world"})
@sh curl "https://{url}" -o "output.txt"
*/

func extractTokenUntilTerminators(runes []rune, startRuneIndex int, term []rune) (token string, endIndex int, terminator rune) {
	if startRuneIndex >= len(runes) {
		return "", len(runes), 0
	}
	if len(term) == 0 {
		return "", len(runes), 0
	}
	var end int
	for end = startRuneIndex; end < len(runes); end++ {
		if slices.Contains(term, runes[end]) {
			return string(runes[startRuneIndex:end]), end, runes[end]
		}
	}

	// No terminator found
	return string(runes[startRuneIndex:]), len(runes), 0
}

func isPositionEscaped(runes []rune, pos int) bool {
	if pos == 0 {
		return false
	}
	if runes[pos-1] == '\\' {
		return !isPositionEscaped(runes, pos-1)
	}
	return false
}

func removeLastRune(sb *strings.Builder) {
	s := sb.String()
	runes := []rune(s)
	if len(runes) == 0 {
		return
	}
	sb.Reset()
	sb.WriteString(string(runes[:len(runes)-1]))
}
