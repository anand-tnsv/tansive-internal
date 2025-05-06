package session

import (
	"fmt"
	"strings"
	"testing"
)

/*
@sh echo "Hello, World!"
@sh echo "{hello}"
@sh "echo {hello}"
@sh "echo \"{hello}\""
@sh ("echo {hello}", @{r}, {"hello": "world"})
@sh curl "https://{url}" -o "output.txt"
*/

func TestParseLine(t *testing.T) {
	line := `@sh @sh ("echo {hello} @{p} (\@get(/my/name))", @{r}, \
	@get(/some/path), {"hello": "world"})`
	token, err := parseLine(line)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	t.Logf("%s\n", printTokenTree(token))
}

func printTokenTree(token *Token) string {
	if token == nil {
		return ""
	}
	if token.Type == TokenTypeString {
		return token.Value
	}
	var ss []string
	for _, child := range token.children {
		ss = append(ss, printTokenTree(child))
	}
	if token.Type == TokenTypeCmd {
		return fmt.Sprintf("<CMD-%s(%s)>", token.Value, strings.Trim(strings.Join(ss, ""), " "))
	} else if token.Type == TokenTypeShell {
		return fmt.Sprintf("<SHELL(%s)>", strings.Trim(strings.Join(ss, ""), " "))
	} else if token.Type == TokenTypeVar {
		return fmt.Sprintf("<VAR(%s)>", strings.Trim(strings.Join(ss, ""), " "))
	}
	return fmt.Sprintf("<%s(%s)>", token.Type, strings.Trim(strings.Join(ss, ""), " "))
}
