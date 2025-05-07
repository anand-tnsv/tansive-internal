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

	//TODO: When @sh begins with (, the ( should not be included in the token
	line := `@sh ("echo {hello} @{p} (\@get(/my/name))", @{r}, 
				@get(/some/path), {"hello": "world"})`

	/*
		line := `
			   const person = {
			     name: "Alice",
			     age: 28,
			     hobbies: ["reading", "biking", "chess"]
			   };

			   // Add a new hobby
			   person.hobbies.push("gardening");

			   // Function to greet the person
			   function greet(p) {
			     console.log("Hello, ${p.name}!");
			   }

			   // Greet the person
			   greet(person);

			   // Check age category
			   if (person.age < 18) {
			     console.log("You're a minor.");
			   } else if (person.age < 65) {
			     console.log("You're an adult.");
			   } else {
			     console.log("You're a senior.");
			   }

			   // Print hobbies
			   for (let hobby of person.hobbies) {
			     console.log("Hobby:", hobby);
			   }
			   `
	*/
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
	for _, child := range token.Children {
		ss = append(ss, printTokenTree(child))
	}
	if token.Type == TokenTypeDocument {
		return fmt.Sprintf("<DOC(%s)>", strings.Trim(strings.Join(ss, ""), " "))
	} else if token.Type == TokenTypeCmd {
		return fmt.Sprintf("<CMD-%s(%s)>", token.Value, strings.Trim(strings.Join(ss, ""), " "))
	} else if token.Type == TokenTypeShell {
		return fmt.Sprintf("<SHELL(%s)>", strings.Trim(strings.Join(ss, ""), " "))
	} else if token.Type == TokenTypeVar {
		return fmt.Sprintf("<VAR(%s)>", strings.Trim(strings.Join(ss, ""), " "))
	}
	return fmt.Sprintf("<%s(%s)>", token.Type, strings.Trim(strings.Join(ss, ""), " "))
}
